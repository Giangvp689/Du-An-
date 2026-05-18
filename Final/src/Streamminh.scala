package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Streamminh {
  def main(args: Array[String]): Unit = {

    // Tạo SparkSession
    val spark = SparkSession.builder()
      .appName("Real Madrid Cards Per File - Streaming")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Khai báo schema tương ứng với file dữ liệu 
    val schema = StructType(List(
      StructField("Div", StringType, true),
      StructField("Date", StringType, true),
      StructField("Time", StringType, true),
      StructField("HomeTeam", StringType, true),
      StructField("AwayTeam", StringType, true),
      StructField("FTHG", IntegerType, true),
      StructField("FTAG", IntegerType, true),
      StructField("FTR", StringType, true),
      StructField("HTHG", IntegerType, true),
      StructField("HTAG", IntegerType, true),
      StructField("HTR", StringType, true),
      StructField("HS", IntegerType, true),
      StructField("AS", IntegerType, true),
      StructField("HST", IntegerType, true),
      StructField("AST", IntegerType, true),
      StructField("HF", IntegerType, true),
      StructField("AF", IntegerType, true),
      StructField("HC", IntegerType, true),
      StructField("AC", IntegerType, true),
      StructField("HY", IntegerType, true),
      StructField("AY", IntegerType, true),
      StructField("HR", IntegerType, true),
      StructField("AR", IntegerType, true)
    ))
    val dstream = spark.readStream
      .format("csv")
      .option("header", true)
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .load("D:/Kiến thức cơ sở ngành/Big Data/Streamming")

    // Tạo bảng tạm để thực hiện SQL
    dstream.createOrReplaceTempView("matches")
    // Truy vấn tổng số thẻ vàng và thẻ đỏ của Real Madrid (tách theo Home/Away)
    val result = spark.sql(
      """ SELECT
    SUM(CASE WHEN HomeTeam = 'Real Madrid' THEN HY ELSE 0 END) +
    SUM(CASE WHEN AwayTeam = 'Real Madrid' THEN AY ELSE 0 END)
    AS total_yellow_cards,

    SUM(CASE WHEN HomeTeam = 'Real Madrid' THEN HR ELSE 0 END) +
    SUM(CASE WHEN AwayTeam = 'Real Madrid' THEN AR ELSE 0 END)
    AS total_red_cards
    FROM matches """
    )
    // Ghi kết quả ra console ở chế độ complete (có aggregation)
    val query = result.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
    query.awaitTermination()
  }
}
