package com.vikas.spark

import org.apache.spark._
import org.apache.log4j._
import scala.math._

/** Find the date which had most precipitation */
object MostPrecipitation {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationId = fields(0)
    val date = fields(1).toInt
    val entryType = fields(2)
    val precipitation = fields(3).toInt
    (stationId, date, entryType, precipitation) 
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // set the log level to only print the errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPrecipitation")
    
    // Read the each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationId, date, entryType, precipitation) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all the PRCP entries
    val allPrcp = parsedLines.filter(x => x._3 == "PRCP")
    
    //val date = allPrcp.map(x => x._2.toInt)
    
    // Convert to (stationId, date, precipitation)
    val prcpByDate = allPrcp.map(x => (x._1, x._4.toInt))
    
    // reduce by station the most precipitation
    val mostPrcpByDate = prcpByDate.reduceByKey((x,y) => max(x,y))
    
    // date of most precipitation
    //val mostPrcpByDate = mostPrcpByStationId.map(x => (x._1,x._2))
    
    // Collect and print the results
    val results = mostPrcpByDate.collect()
    
    for (result <- results.sorted) {
      val stationID = result._1
      val prcp = result._2
      println(s"Date: $stationID most Precipitation: $prcp")
    }  
  }
}