package part3dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object DataSetsOptions extends App {

  val spark = SparkSession.builder()
    .appName("DataSetsOptions")
    .master("local")
    .getOrCreate()

  val numberDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("app/resources/numbers.csv")

  numberDf.show()

  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt

  val numbersDs: Dataset[Int] = numberDf.as[Int]

  numbersDs.filter( _ % 2 == 0)

  // working with complex types
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
  )


  def readFileSpark(path: String, formatType: String): DataFrame =spark.read
    .format(formatType)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(path)

  val carDf = readFileSpark("app/resources/cars.json", "json")

  // implicit val carEncoder: Encoder[Car] = Encoders.product[Car]
  // or

  import spark.implicits._
  val carDataset: Dataset[Car] = carDf.as[Car]

  carDataset.count()

  carDataset.filter( car => car.Horsepower.getOrElse(0L) > 140).count()

  val avgHP: Long = carDataset.map(car =>
    car.Horsepower.getOrElse(0L)).reduce((x, y) => x + y ) / carDataset.count()

  println(avgHP)

  // join dataset
  case class GuitarPlayer( id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Guitar(id: Long, model: String, make: String, guitarType: String)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarPlayerDF = readFileSpark("app/resources/guitarPlayers.json", "json")
  val guitarPlayerDataSet: Dataset[GuitarPlayer] = guitarPlayerDF.as[GuitarPlayer]

  val guitarDF = readFileSpark("app/resources/guitars.json", "json")
  val guitarDataset = guitarDF.as[Guitar]

  val bandDF = readFileSpark("app/resources/bands.json", "json")
  val bandDataset = bandDF.as[Band]

  val guitarPlayerAndBand = guitarPlayerDataSet.joinWith(bandDataset, guitarPlayerDataSet.col("band") === bandDataset.col("id"))

  guitarPlayerAndBand //.show()

  val guitarPlayerAndGuitar = guitarPlayerDataSet.joinWith(guitarDataset, array_contains($"guitars", guitarDataset.col("id")), "outer")

  guitarPlayerAndGuitar.show()

  // group

  carDataset.groupByKey(_.Origin).count().show()









}
