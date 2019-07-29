package com.vikas.spark

import org.apache.spark._
import org.apache.log4j._
import scala.math._

object AvgSalary {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val id = fields(0)
    val sal = fields(1)
    (id,sal)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "AvgSalary")
    
    val lines = sc.textFile("../salary.txt")
    
    val parsedLines = lines.map(parseLine)
    
    val cut = parsedLines.map(x => (x._1))
    
    val length = cut.map(x => (x,1)).reduceByKey((x,y) => x+y)
    
   // val salary = parsedLines.map(x => (x._1, x._2.toFloat))
    
    
    //val avgSalary = salary.reduceByKey((x,y) => (x+y)/length)
    
   val results = length.collect()
    
    results.foreach(println)
    
  }
}