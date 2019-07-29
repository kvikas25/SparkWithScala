package com.vikas.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object InpatientCharge {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
        .appName("SparkSQL")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///c:/temp")
        .getOrCreate()
        
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true")
    .option("inferschema", "true").load("../inpatientCharges.csv")
    
    //df.show()
    
    df.write.parquet("PatientCharges.parquet")
    
    //df.registerTempTable("hospital_charges")
    
   // df.groupBy("ProviderState").avg("AverageCoveredCharges").show() //write.parquet("accByState.parquet")
    
    val newDataDF = spark.read.parquet("PatientCharges.parquet")
    
    newDataDF.show()
    
    
    
    
  }
  
}