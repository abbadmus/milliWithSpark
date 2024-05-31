import mill._, scalalib._

object app extends ScalaModule {

  def scalaVersion = "2.13.12"

  def ivyDeps: T[Agg[Dep]] = super.ivyDeps() ++ Agg(
    ivy"com.lihaoyi::os-lib::0.9.0",
    ivy"org.apache.spark:spark-core_2.13:3.5.1",
    ivy"org.apache.spark:spark-sql_2.13:3.5.1",
    // ivy"org.apache.spark:spark-sql-api_2.13:3.5.1",

    // logging
    ivy"org.apache.logging.log4j:log4j-api-scala_2.13:12.0",
    ivy"org.apache.logging.log4j:log4j-core:2.13.3",
    // postgres for DB connectivity
    ivy"org.postgresql:postgresql:42.7.3"
  )

  object test extends ScalaTests {
    def ivyDeps: T[Agg[Dep]] = Agg(ivy"com.lihaoyi::utest:0.7.11")
    def testFramework = "utest.runner.Framework"
  }

  def mainClass = Some("part6practical.TestDeployApp")

  def prependShellScript = ""


  // Custom target for creating a JAR for
//  object part6practical extends ScalaModule {
//    def scalaVersion = app.scalaVersion
//    def moduleDeps = Seq(app)
//    def mainClass = Some("app.part6practical.TestDeployApp")
//
//    def prependShellScript = ""
//
//  }









}
