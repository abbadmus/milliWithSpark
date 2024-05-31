package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpression extends App {

  val spark = SparkSession
    .builder()
    .appName("ColumnsAndExpression")
    .config("spark.master", "local")
    .getOrCreate()

  val carDf = spark.read
    .format("json")
    .option("inferSchema",  "true")
    .load("./app/resources/cars.json")

  carDf.show()

  import spark.implicits._

  carDf.select(
    col("Acceleration"),
    col("Displacement"),
    column("Miles_per_Gallon"),
    $"Horsepower",
    $"Year",
    expr(("Origin"))
  )//.show()

  val addingAccx10 = carDf.select(
    col("Acceleration"),
    col("Displacement"),
    column("Miles_per_Gallon"),
    (column("Acceleration") * 10).as("moreAcc")
  )

  addingAccx10.show()


}
