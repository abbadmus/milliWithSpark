package part2dataframes

import org.apache.spark.sql.SparkSession

object Exercise1 extends App {
  val spark = SparkSession.builder()
    .appName("Exercise1")
    .config("spark.master", "local")
    .getOrCreate()

  val phone = Seq(
    ("iphone", "XR", "6by4", 6),
    ("iphone", "x", "6by5", 12),
    ("Samsung", "galaxy5", "6by5", 13),
  )

  import spark.implicits._
  val phoneDF = phone.toDF("make", "model", "screen dimension", "camera megapixels")

  phoneDF.show()

  val movieDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("most", "fastFail") // dropMalFormed or permissive(default)
    .load("./app/resources/movies.json")

  movieDF.show()

  println(movieDF.count())

}
