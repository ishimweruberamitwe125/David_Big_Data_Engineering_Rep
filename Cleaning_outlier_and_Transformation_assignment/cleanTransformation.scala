package org.it.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
object cleanTransformation extends App {
  //Create Spark session
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DataFrameDemo")
  sparkconf.set("spark.master", "local[1]")
  val ss = SparkSession.builder().config(sparkconf).getOrCreate()
  //Define schema
  val ddlSchema =
    """
      product_number STRING,
      product_name STRING,
      product_category STRING,
      product_scale STRING,
      product_manufacturer STRING,
      product_description STRING,
      length DOUBLE,
      width DOUBLE,
      height DOUBLE
    """
  //Load DataFrame
  val cleandf = ss.read.option("header", true).schema(ddlSchema).csv("C:/Users/David Ruberamitwe/Downloads/products.csv")
  //1.Cleaning - Check for outliers in `length` and `width`
  val lengthStats = cleandf.select(F.mean("length").as("mean_length"), F.stddev("length").as("stddev_length")).first()
  val widthStats = cleandf.select(F.mean("width").as("mean_width"), F.stddev("width").as("stddev_width")).first()
  //Define boundaries for outliers based on Z-score (mean ± 3*stddev)
  val lengthLowerBound = lengthStats.getAs[Double]("mean_length") - 3 * lengthStats.getAs[Double]("stddev_length")
  val lengthUpperBound = lengthStats.getAs[Double]("mean_length") + 3 * lengthStats.getAs[Double]("stddev_length")
  val widthLowerBound = widthStats.getAs[Double]("mean_width") - 3 * widthStats.getAs[Double]("stddev_width")
  val widthUpperBound = widthStats.getAs[Double]("mean_width") + 3 * widthStats.getAs[Double]("stddev_width")
  //Filter out outliers
  val filteredDf = cleandf.filter(F.col("length").between(lengthLowerBound, lengthUpperBound) && F.col("width").between(widthLowerBound, widthUpperBound))
  //2.Cleaning - Separate `product_number` into `storeid` and `productid`
  val separatedDf = filteredDf.withColumn("storeid", F.split(F.col("product_number"), "_").getItem(0)).withColumn("productid", F.split(F.col("product_number"), "_").getItem(1))
  //3.Cleaning - Extract `year` from `product_name`
  val yearExtractedDf = separatedDf.withColumn("year", F.regexp_extract(F.col("product_name"), "\\b(\\d{4})\\b", 1))
  //checking cleaned df after all cleaning required for the question
  yearExtractedDf.show(numRows=5)
  //1.Transformation - Add `product_size` column
  val transformedDf = yearExtractedDf.withColumn("product_size",
    F.when(F.col("length") < 1000, "Small")
      .when(F.col("length").between(1000, 2000), "Medium")
      .when(F.col("length").between(2000, 3000), "Large")
      .otherwise("Extra Large"))
  //2.Transformation - Create pivot based on `product_category` and `product_size` and count the products
  val pivotDf = transformedDf.groupBy("product_category").pivot("product_size").count()
  pivotDf.show()
  //3.Transformation - Use a window function to rank products within each category based on their length
  val windowSpec = Window.partitionBy("product_category").orderBy(F.desc("length"))
  val rankedDf = transformedDf.withColumn("rank", F.row_number().over(windowSpec))
    .filter(F.col("rank") === 2)
    .select("product_category", "product_name", "length")
  rankedDf.show()
  //Write cleaned data to HDFS or local disk (if required)
  transformedDf.coalesce(1).write.csv("C:/Users/David Ruberamitwe/Downloads/DataPipeline/CleanedData1")
  //Stop Spark session
  ss.stop()
}

