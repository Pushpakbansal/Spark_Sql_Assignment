package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.sql.Date._ 
import org.apache.spark.sql.types.{DateType, IntegerType}

object SparkAssign {
  
  case class Products(Product_id:Int, Product_category_id:Int, Product_name:String, Product_desc:String, Product_price:Float, Product_img:String)
  
  def mapper1(line:String): Products = {
    val fields = line.split(';')  
    
    val products:Products = Products(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4).toFloat, fields(5))
    return products
  }
   
  case class OrderItem(Order_item_id:Int, Order_item_order_id:Int, Order_item_product_id:Int, Order_item_quantity:Int, Order_item_subtotal:Float, Order_item_prod_price:Float)
  
  def mapper2(line:String): OrderItem = {
    val fields = line.split(',')  
    
    val orderItem:OrderItem = OrderItem(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toFloat, fields(5).toFloat)
    return orderItem
  }  
  
  // To be done
  case class Orders(Order_id:Int, Order_dt:String , Order_customer_id:Int, order_status:String)
  
  def mapper3(line:String): Orders = {
    val fields = line.split(',')  
    
    val orders:Orders = Orders(fields(0).toInt, fields(1), fields(2).toInt, fields(3))
    return orders
  }
 
  case class Customers(Customer_id:Int ,Customer_fname:String, Customer_lname:String, Customer_email:String, Customer_pwd:String, Customer_street:String, Customer_city:String, Customer_state:String, Customer_zipcode:String)
  
  def mapper4(line:String): Customers = {
    val fields = line.split(';')  
    
    val customers:Customers = Customers(fields(0).toInt, fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8))
    return customers
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // This is for the local machine, so in the E drive.
    //val products = spark.sparkContext.textFile("E:/Spark_Scala/SparkScala_Documents/Products.txt")
    val products = spark.sparkContext.textFile("https://github.com/Pushpakbansal/Spark_Sql_Assignment/blob/master/Products.txt")
    val customers= spark.sparkContext.textFile("https://github.com/Pushpakbansal/Spark_Sql_Assignment/blob/master/Customers.txt")
    val orders= spark.sparkContext.textFile("https://github.com/Pushpakbansal/Spark_Sql_Assignment/blob/master/Orders.txt")
    val ordersitem= spark.sparkContext.textFile("https://github.com/Pushpakbansal/Spark_Sql_Assignment/blob/master/Order_items.txt")
    
    val products_rdd = products.map(mapper1)
    val ordersitem_rdd = ordersitem.map(mapper2)
    val orders_rdd = orders.map(mapper3)
    val customers_rdd = customers.map(mapper4)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    
    val schemaCustomers = customers_rdd.toDS
    val schemaProducts = products_rdd.toDS
    val schemaOrders = orders_rdd.toDS
    val schemaOrdersitems = ordersitem_rdd.toDS
    
//    schemaCustomers.printSchema()
//    schemaProducts.printSchema()
//    schemaOrders.printSchema()
//    schemaOrdersitems.printSchema()
    
    // SQL can be run over DataFrames that have been registered as a table
    schemaCustomers.createOrReplaceTempView("Customers")
    schemaProducts.createOrReplaceTempView("Products")
    schemaOrders.createOrReplaceTempView("Orders")
    schemaOrdersitems.createOrReplaceTempView("Order_Item")    
    
    // Total sales for each date (hint: join orders & order_items) 
    val sum_sale_date = spark.sql("SELECT Orders.Order_dt,sum(Order_item_subtotal) FROM Orders JOIN Order_Item on Order_Item.Order_item_order_id = Orders.Order_id group by Orders.Order_dt")
    
    // Total sales for each month
    val sum_sale_month = spark.sql("SELECT mth, sum(sales) from (SELECT month(to_date(o.Order_dt)) as mth, Order_item_subtotal as sales FROM Orders o JOIN Order_Item o_i on o_i.Order_item_order_id = o.Order_id) tbl_1 group by mth")
    
    /**
     * Average sales for each date
     * 1. In the Inner query get the  
     */
    val avg_sale_date = spark.sql("SELECT Orders.Order_dt,avg(Order_item_subtotal) FROM Orders JOIN Order_Item on Order_Item.Order_item_order_id = Orders.Order_id group by Orders.Order_dt")
    
    // Average sales for month
    val avg_sale_month = spark.sql("SELECT mth, avg(sales) from (SELECT month(to_date(o.Order_dt)) as mth, Order_item_subtotal as sales FROM Orders o JOIN Order_Item o_i on o_i.Order_item_order_id = o.Order_id) tbl_1 group by mth")
    
    // Top 10 revenue generating products
    val top_products = spark.sql("Select Product_name,Product_id from (select Order_item_product_id from (select Order_item_product_id, dense_rank() over (order by sales) as rnk from (select Order_item_product_id, sum(Order_item_subtotal) as sales from Order_Item group by Order_item_product_id) tbl_1) tbl_2 where rnk <= 10) tbl_3 JOIN Products on Order_item_product_id = Product_id") 
    
    // Top 3 purchased customers for each month
    val top_customer = spark.sql("select mth, c_id, rnk, cnt from (select mth, c_id, dense_rank() over(partition by mth order by cnt desc) as rnk, cnt from (Select month(to_date(Order_dt)) as mth, Customer_id as c_id, count(Order_customer_id) as cnt from Orders o JOIN Customers c on o.Order_customer_id = c.Customer_id group by month(to_date(Order_dt)),Customer_id ) tbl_1) tbl_2 where rnk <=3")
    //top_customer.printSchema()
    
    // Top produce by sale for each month
    val top_prod_by_sale = spark.sql("select mth,Product_name from( select mth, p_id from (select mth, p_id, dense_rank() over (partition by mth, p_id order by sales desc) as rnk from (select mth, p_id, sum(qty) sales from (select Order_item_product_id as p_id, Order_item_quantity as qty , month(to_date(Order_dt)) as mth From Orders od JOIN Order_Item oi on od.Order_id = oi.Order_item_order_id) tbl_1 group by mth,p_id) tbl_2) tbl_3 where rnk = 1) tbl_4 JOIN Products on tbl_4.p_id = Products.Product_id")
    
    // To Count of distinct Customer, group by State we used the Customer_id as this is the primary key 
    val dist_cust = spark.sql("select Customer_state, count(Customer_id) from Customers group by Customer_state")
    
    // Most popular product category 
    val popular_prod_ctg = spark.sql("select p.Product_category_id from ( select p_id, dense_rank() over (order by sales) as rnk from (select Order_item_product_id as p_id, sum(Order_item_quantity) as sales from Order_Item group by Order_item_product_id) tbl_1 ) tbl_2 JOIN Products p where p_id = p.Product_id and rnk =1")
    
    /*
     * Try any of the above and check the results below
     * */
    val results = top_products.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}