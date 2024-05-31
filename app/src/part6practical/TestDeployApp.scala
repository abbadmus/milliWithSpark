package part6practical

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TestDeployApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      println("we need input and output paths")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("TestDeployApp")
      .getOrCreate()

    def readFileSpark(path: String, formatType: String): DataFrame = spark.read
      .format(formatType)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

    val movieDf = readFileSpark(args(0), "json")

    val goodComedy = movieDf.select(
      col("Title"), col("IMDB_Rating").as("rating"),
      col("IMDB_Votes").as("Votes")
    )
      .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5 )
      .sort(col("rating").desc_nulls_last)

    goodComedy.show()

    goodComedy.write.mode(SaveMode.Overwrite).json(args(1))

  }

  // goodComedy.write.mode(SaveMode.Overwrite).json("app/resources/goodMovie") //args(1)
  // val movieDf = readFileSpark("app/resources/movies.json", "json")

  // ./bin/spark-submit --class app.part6practical.TestDeployApp --deploy-mode client --master spark://119db539738a:7077 --verbose --supervise /opt/spark-apps/out.jar /opt/spark-data/movies.json /opt/spark-data/goodComedies
}


