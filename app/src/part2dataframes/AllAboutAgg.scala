package part2dataframes

import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object AllAboutAgg extends App {
  val spark = SparkSession.builder()
    .appName("AllAboutAgg")
    .master("local")
    .getOrCreate()

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


  val movieSchema = StructType(
    Array(
      StructField("Title", StringType),
      StructField("US_Gross", DoubleType),
      StructField("Worldwide_Gross", IntegerType),
      StructField("US_DVD_Sales", DoubleType),
      StructField("Production_Budget", DoubleType),
      StructField("Release_Date", DateType),
      StructField("MPAA_Rating", StringType),
      StructField("Running_Time_min", DoubleType),
      StructField("Distributor", StringType),
      StructField("Source", StringType),
      StructField("Major_Genre", StringType),
      StructField("Creative_Type", StringType),
      StructField("Director", StringType),
      StructField("Rotten_Tomatoes_Rating", DoubleType),
      StructField("IMDB_Rating", DoubleType),
      StructField("IMDB_Votes", IntegerType),
    )
  )

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val movieDf = spark.read
    .schema(movieSchema)
    .option("dateFormat", "dd-MMM-yy") // 7-Aug-98
    .json("./app/resources/movies.json")

  movieDf.show()

  val numberOfGenres = movieDf.select(count("Major_Genre")) // all value except nulls
  numberOfGenres//.show()

  val numberOfGenres2 = movieDf.selectExpr("count(Major_Genre)") // all value except nulls
  numberOfGenres2//.show()

  movieDf.select(count("*")) // with null

  movieDf.select(countDistinct("Major_Genre")).show()

  movieDf.select(approx_count_distinct("Major_Genre")).show()

  movieDf.select(mean("Worldwide_Gross"))
  movieDf.select(sum("Worldwide_Gross"))
  movieDf.selectExpr("sum(Worldwide_Gross)")//.show()

  // grouping

  val countByGenre  = movieDf.groupBy("Major_Genre").count() // include nulls
  countByGenre.show()

  val aggByGenre  = movieDf.groupBy("Major_Genre")
    .agg(
      count("*").as("N_movie"),
      round(avg("IMDB_Rating"), 2).as("Avg_IMDB_Rating")
    ).orderBy("Avg_IMDB_Rating")

  aggByGenre.show()

  // assignment
  movieDf.select(
    (col("Worldwide_Gross") +
    col("Worldwide_Gross") +
    col("US_DVD_Sales")).as("sum_to_gross")
  ).select(sum("sum_to_gross").as("sum_to_gross")).show()

//  +----------------+
//  | sum_to_gross |
//  +----------------+
//  |
//  1.77703399672E11 |
//  +----------------+

  movieDf.select(countDistinct("Director")).show()
  movieDf.select(mean("US_Gross"), std(col("US_Gross"))).show()
  movieDf.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross")
    ).orderBy(col("Avg_IMDB_Rating").desc_nulls_last)
    .show()









}
