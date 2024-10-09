package org.it.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Spark Configuration and Session Setup
object miniProject extends App {
  // Spark configuration
  val sparkConf = new SparkConf()
    .set("spark.app.name", "DataFrameDemo")
    .set("spark.master", "local[1]")

  // Spark session
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  // MySQL connection properties
  val jdbcUrl = "jdbc:mysql://localhost:3306/challenge1"
  val connectionProperties = new java.util.Properties()
  connectionProperties.setProperty("user", "root")
  connectionProperties.setProperty("password", "IshimweRuberamitwe@125")
  connectionProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

  // Reading MySQL tables into DataFrames
  val salesDF: DataFrame = spark.read.jdbc(jdbcUrl, "sales", connectionProperties)
  val menuDF: DataFrame = spark.read.jdbc(jdbcUrl, "menu", connectionProperties)
  val membersDF: DataFrame = spark.read.jdbc(jdbcUrl, "members", connectionProperties)

  // Question 1: Total sum of prices per customer
  val question1 = salesDF
    .join(menuDF, salesDF("product_id") === menuDF("product_id"), "left")
    .groupBy("customer_id")
    .agg(sum("price").alias("total_spent"))
  question1.show()

  // Question 2: Count of unique order dates per customer
  val question2 = salesDF
    .select("customer_id", "order_date")
    .distinct()
    .groupBy("customer_id")
    .agg(countDistinct("order_date").alias("unique_order_dates"))
  question2.show()

  // Question 3: First product purchased by each customer using window functions
  import org.apache.spark.sql.expressions.Window
  val windowSpec = Window.partitionBy("customer_id").orderBy("customer_id")
  val question3 = salesDF
    .join(menuDF, salesDF("product_id") === menuDF("product_id"), "inner")
    .withColumn("row_first", row_number().over(windowSpec))
    .filter(col("row_first") === 1)
    .select("customer_id", "product_name")
  question3.show()

  // Question 4: Most purchased product
  val question4= salesDF
    .groupBy("product_id")
    .agg(count("*").alias("total_purchases"))
    .orderBy(desc("total_purchases"))
    .limit(1)
    .join(menuDF, "product_id")
    .select("product_name", "total_purchases")
  question4.show()

  // Question 5: Most frequently purchased product per customer
  val productFrequencyPerCustomer = salesDF
    .join(menuDF, "product_id")
    .groupBy("customer_id", "product_name")
    .agg(count("product_id").alias("frequency"))

  val maxFrequencyPerCustomer = productFrequencyPerCustomer
    .groupBy("customer_id")
    .agg(max("frequency").alias("max_frequency"))

  val question5 = productFrequencyPerCustomer
    .join(maxFrequencyPerCustomer, Seq("customer_id"))
    .where(col("frequency") === col("max_frequency"))
    .orderBy("customer_id")
    .select("customer_id", "product_name", "frequency")
  question5.show()
  // Question 6
  salesDF.createOrReplaceTempView("sales")
  menuDF.createOrReplaceTempView("menu")
  membersDF.createOrReplaceTempView("members")

  // Question 6: First purchase on or after the join date
  // Question 6 using Spark SQL
  val question6 = spark.sql("""
  WITH Rank1 AS (
    SELECT
      s.customer_id,
      m.product_name,
      me.join_date,
      s.order_date,
      MIN(s.order_date) OVER (PARTITION BY s.customer_id) AS min_order_date
    FROM sales s
    JOIN menu m ON m.product_id = s.product_id
    JOIN members me ON me.customer_id = s.customer_id
    WHERE s.order_date >= me.join_date
  )
  SELECT customer_id, product_name, join_date, order_date
  FROM Rank1
  WHERE order_date = min_order_date
  ORDER BY customer_id
""")
  question6.show()


  // Question 7
  // Question 7 using Spark SQL
  val question7 = spark.sql("""
  WITH Rank1 AS (
    SELECT
      s.customer_id,
      m.product_name,
      DENSE_RANK() OVER (PARTITION BY s.customer_id ORDER BY s.order_date) AS rank
    FROM sales s
    JOIN menu m ON m.product_id = s.product_id
    JOIN members me ON me.customer_id = s.customer_id
    WHERE s.order_date < me.join_date
  )
  SELECT customer_id, product_name
  FROM Rank1
  WHERE rank = 1
  ORDER BY customer_id
""")
  question7.show()


  // Question 8
  val question8 = spark.sql("""
    SELECT s.customer_id, COUNT(s.product_id) AS total_items_purchased,
      SUM(m.price) AS total_amount_spent
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members me ON me.customer_id = s.customer_id
    WHERE s.order_date < me.join_date
    GROUP BY s.customer_id
  """)
  question8.show()

  // Question 9
  val question9 = spark.sql("""
    SELECT s.customer_id, SUM(CASE
      WHEN m.product_id = 1 THEN m.price * 20
      ELSE m.price * 10
    END) AS points1
    FROM sales s
    JOIN (SELECT product_id, price, CASE
      WHEN product_id = 1 THEN price * 20
      ELSE price * 10
    END AS points FROM menu) m ON s.product_id = m.product_id
    GROUP BY s.customer_id
  """)
  question9.show()

  // Question 10
  val question10 = spark.sql("""
  SELECT
    s.customer_id,
    SUM(CASE
      WHEN (DATEDIFF(me.join_date, s.order_date) BETWEEN 0 AND 7) OR (m.product_id = 1)
        THEN m.price * 20
      ELSE m.price * 10
    END) AS total_points
  FROM sales s
  JOIN menu m ON s.product_id = m.product_id
  JOIN members me ON me.customer_id = s.customer_id
  WHERE s.order_date >= me.join_date
    AND s.order_date <= '2021-01-31'
  GROUP BY s.customer_id
  ORDER BY s.customer_id
""")
  question10.show()


  // Write the outputs to MySQL
  def writeToDatabase(df: DataFrame, tableName: String): Unit = {
    df.write
      .mode("overwrite")
      .jdbc(jdbcUrl, tableName, connectionProperties)
  }

  // Writing outputs back to MySQL
  writeToDatabase(question1, "question1_output")

  writeToDatabase(question2, "question2_output")
  writeToDatabase(question3, "question3_output")
  writeToDatabase(question4, "question4_output")
  writeToDatabase(question5, "question5_output")
  writeToDatabase(question6, "question6_output")
  writeToDatabase(question7, "question7_output")
  writeToDatabase(question8, "question8_output")
  writeToDatabase(question9, "question9_output")
  writeToDatabase(question10, "question10_output")
  val checkData = spark.read.jdbc(jdbcUrl, "question10_output", connectionProperties)
  checkData.show(10)


  // Close the Spark session
  spark.stop()
}

