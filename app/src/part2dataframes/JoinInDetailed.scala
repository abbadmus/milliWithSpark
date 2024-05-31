package part2dataframes

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinInDetailed extends App {
  val spark = SparkSession
    .builder()
    .appName("JoinInDetailed")
    .master("local")
    .getOrCreate()

  val guitarPlayersDF: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .json("./app/resources/guitarPlayers.json")

  val guitarDF: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .json("./app/resources/guitars.json")

  val bandsDF: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .json("./app/resources/bands.json")


  // inner joint
  val joinBandGuitarCondition = bandsDF.col("id") === guitarPlayersDF.col("band")
  val guitarAndBandDFInner = guitarPlayersDF.join(bandsDF, joinBandGuitarCondition, "inner")

  guitarAndBandDFInner.show()

  // left_outer joint
  val guitarAndBandDFLeftOuter = guitarPlayersDF.join(bandsDF, joinBandGuitarCondition, "left_outer")

  guitarAndBandDFLeftOuter.show()

  val guitarAndBandDFOuter = guitarPlayersDF.join(bandsDF, joinBandGuitarCondition, "outer")

  guitarAndBandDFOuter.show()

  println()
  println()

  guitarPlayersDF.show()
  guitarPlayersDF.join(bandsDF, joinBandGuitarCondition, "left_semi").show()

  guitarPlayersDF.join(bandsDF, joinBandGuitarCondition, "left_anti").show()

  println()

  // option 1 when we have same column name in the two join tables
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2
  guitarAndBandDFLeftOuter.drop(bandsDF.col("id")).show()

  guitarPlayersDF
    .join(guitarDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()








}

//+----+-------+---+------------+
//|band|guitars| id|        name|
//+----+-------+---+------------+
//|   0|    [0]|  0|  Jimmy Page|
//|   1|    [1]|  1| Angus Young|
//|   2| [1, 5]|  2|Eric Clapton|
//|   3|    [3]|  3|Kirk Hammett|
//+----+-------+---+------------+

//+----+-------+---+------------+
//|band|guitars| id|        name|
//+----+-------+---+------------+
//|   0|    [0]|  0|  Jimmy Page|
//|   1|    [1]|  1| Angus Young|
//|   3|    [3]|  3|Kirk Hammett|
//+----+-------+---+------------+