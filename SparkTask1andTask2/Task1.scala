package org.it.com

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Task1 extends App {
  val sc = new SparkContext("local[1]","AppName")// Initialize SparkContext
  val rdd1 = sc.textFile("C:/Users/David Ruberamitwe/Downloads/input.txt")// Read the input text file
  val parsedData = rdd1.map(line => line.split(","))// Split each line by commas,validate and extract sensor_id, date,and temperature
    .filter(parts => parts.length == 3)// Filter out lines that do not have exactly 3 element
    .map(parts => {
      val sensorId = parts(0)
      val date = parts(1)
      val temp = parts(2).toFloat // Convert temperature to Float
      (sensorId, date, temp)
    })
  // Find the maximum temperature from the parsed data
  val maxTemp = parsedData.map(_._3).max()
  println(s"Maximum temperature recorded: $maxTemp")
  // Stop the SparkContext
  sc.stop()
}
