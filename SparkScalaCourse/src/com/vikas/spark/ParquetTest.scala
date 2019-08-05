package com.vikas.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SQLContext._


object ParquetTest {
  
  case class Student(ID:Int, first_name:String, last_name:String, age:Int, marks:Int)
  
  def mapper(line:String): Student = {
    val fields = line.split(",")
    
    val student:Student = Student(fields(0).toInt, fields(1), fields(2), fields(3).toInt, fields(4).toInt)
    return student
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
        .appName("SparkSQL")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///c:/temp")
        .getOrCreate()
        
    import spark.implicits._   
    val lines = spark.sparkContext.textFile("../student.txt")
    val student = lines.map(mapper).toDS().cache()
    
    val df: DataFrame = student.toDF()
    
    df.write.parquet("marks.parquet")
    
    val newDataDF = spark.read.parquet("marks.parquet")
    
    newDataDF.show()
    
    
    
  }
  
}