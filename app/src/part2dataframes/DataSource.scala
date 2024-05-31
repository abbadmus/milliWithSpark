package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.util.Properties

object DataSource extends App {
  val spark = SparkSession.builder()
    .appName("DataSource")
    .config("spark.master", "local")
    .getOrCreate()

  val carSchama = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  // json
  val myCarDf = spark.read
    .format("json")
    .schema(carSchama)
    .option("dateFormat",  "yyyy-MM-dd")
    .load("./app/resources/cars.json")

  myCarDf.show()

  val stockSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", FloatType)

    )
  )

  // from csv
  val stocksDf = spark.read
    .format("csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("step", ",")
    .load("./app/resources/stocks.csv")

  stocksDf.show()

  // remote db
  val employeeDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable","public.employees")
    .load()

  employeeDf.show()

  stocksDf.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.stock")
    .save()



}
