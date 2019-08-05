package com.vikas.spark

import org.apache.spark._
import org.apache.log4j._

object PurchaseByCustomer {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val price = fields(2).toFloat
    (customerId, price)
  }
  
  def main(args: Array[String])
  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")
    
    val lines = sc.textFile("../customer-orders.csv")
    
    val parsedLines = lines.map(parseLine)
    
    val amountByCustomerId = parsedLines.map(x => (x._1,x._2.toFloat)).reduceByKey((x,y) => x + y)
    
    val sortAmountByCustomerId = amountByCustomerId.map(x => (x._2, x._1)).sortByKey()
    
    val results = sortAmountByCustomerId.collect()
    
    for (result <- results) {
      val temp = result._1
      val customerId = result._2
      val formattedPrice = f"$temp%.2f F"
      println(s"$customerId, $formattedPrice")
  }
    
    
  }
}