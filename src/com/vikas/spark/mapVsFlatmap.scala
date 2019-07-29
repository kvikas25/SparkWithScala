package com.vikas.spark

import org.apache.spark._
import org.apache.log4j._

object mapVsFlatmap {
  
  def main(args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "mapVsFlatmap")
    
    val lines = sc.textFile("../redfox.txt")
    
    val output = lines.map(x => x.toUpperCase())
    
    val output1 = lines.flatMap(x => x.split(" "))
    
    val results = output1.collect();
    
    results.foreach(println)
    
  }
}