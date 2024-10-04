package org.it.com

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Task2 extends App {
  val sc = new SparkContext("local[1]","AppName")// Initialize SparkContext
  val rdd1 = sc.textFile("C:/Users/David Ruberamitwe/Downloads/input.txt")
  val parsedData = rdd1.map(line => line.split(","))// Split each line by commas, validate and extract sensor_id, date, and temperature
    .filter(parts => parts.length == 3) // Filter out lines that do not have exactly 3 elements
    .map(parts => {
      val sensorId = parts(0)
      val temp = parts(2).toFloat // Convert temperature to Float
      (sensorId, temp)
    })
  // Filter temperatures greater than 50 and count occurrences per sensor
  val tempabv50 = parsedData.filter { case (_, temp) => temp > 50 }
  val sensorTempCnt = tempabv50.map { case (sensorId, _) => (sensorId, 1) }
    .reduceByKey(_ + _).sortBy(_._2,ascending = false)
  // Collect the result and print
  sensorTempCnt .collect().foreach { case (sensorId, count) =>
    println(s"$count, $sensorId")
  }
  sc.stop()
}
