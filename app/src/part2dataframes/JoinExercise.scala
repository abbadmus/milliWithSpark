package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.StdIn

object JoinExercise extends App {
  val spark = SparkSession.builder().appName("JoinExercise")
    .master("local")
    .getOrCreate()

  val salaryDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .load()

  val employerDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employerDf.show()

  val eachEmployerWithMaxSalary = salaryDf.groupBy("emp_no")
    .agg(max("salary").as("max_salary"))

  eachEmployerWithMaxSalary.show()

  val joinData = employerDf.join(eachEmployerWithMaxSalary, "emp_no")

  joinData.drop(eachEmployerWithMaxSalary.col("emp_no"))
  joinData.show()

  val deptManagers = spark.read.format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.dept_manager")
    .load()

  val employeeNotManager = employerDf.join(deptManagers, "emp_no", "left_anti")

  employeeNotManager.show()

  



  StdIn.readLine()
}
