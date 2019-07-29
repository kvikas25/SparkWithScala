package com.vikas.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FindSongs {
  
  def parseLine(line:String)={
    val fields = line.split(",")
    val title = fields(0)
    val lname = fields(3)
    (title,lname)
  }
  
  def main(args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "FindSongs")
    
    val lines = sc.textFile("../singer.txt")
    
    val rdd = lines.map(parseLine)
    
    val findSongs = rdd.filter(x => x._2 == "Yagnik").map(x => (x._1,x._2))
    
    val results = findSongs.collect()
    
    for (result <- results.sorted)
    {
      val title = result._1
      val lname = result._2
      println(s"title: $title  Last Name: $lname")
    }
    
    
  }
  
}