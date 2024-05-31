package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameBasics extends App {

  val spark = SparkSession
    .builder()
    .appName("DataFrameBasics")
    .config("spark.master", "local")
    .getOrCreate()

  val readingCarDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("app/resources/cars.json")

  readingCarDf.show()

  readingCarDf.printSchema()

  readingCarDf.take(10).foreach(println)

  val longType = LongType

  val carSchema: StructType = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  println(readingCarDf.schema)

// reading using schema
  val readingCarDfWithSchema = spark.read
    .format("json")
    .schema(carSchema)
    .load("./app/resources/cars.json")

  readingCarDf.show()

  // creating rows by hand
  val myRow = Row("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")

  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val myRow2 = spark.createDataFrame(cars)

  import spark.implicits._
  val manualDf = cars.toDF()


}
