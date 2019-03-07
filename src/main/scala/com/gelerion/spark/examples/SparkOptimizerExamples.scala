package com.gelerion.spark.examples

import com.gelerion.spark.examples.data.{Actor, Movie, Role, domain_data}
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.{SparkConf, SparkContext}

object SparkOptimizerExamples {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("Optimizer Examples")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
  import sqlContext._

  def main(args: Array[String]): Unit = {
    val moviesDb: Seq[Movie] = domain_data.movies
    val acotrsDb: Seq[Actor] = domain_data.actors
    val rolesDb:  Seq[Role]  = domain_data.roles

    val movies = sc.parallelize(moviesDb)
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    movies.registerAsTable("movies")

    //initial plan derived via SqlParser
    explain("SELECT * FROM movies")

    //filter condition
    explain("SELECT name, year FROM movies WHERE year >= 2017")

    //complex sql
    explain(
      """
        |SELECT year, count(year) AS movies_count
        |FROM movies
        |WHERE name LIKE '%a%'
        |GROUP BY year
        |HAVING movies_count >= 1 LIMIT 5
        |"""
        .stripMargin)
  }

  def explain(sql: String): Unit = {
    val data = sqlContext.sql(sql)

    println("SQL:")
    println("----------------------------------------------------------------------------------")
    println(sql.trim)
    println()
    println("Execution Plan:")
    println("----------------------------------------------------------------------------------")
    println(data.queryExecution.executedPlan)

    println("Data:")
    println("----------------------------------------------------------------------------------")
    printTableHeader(data.schemaString)
    data.collect().foreach(println)

    footer
  }


  def printTableHeader(schemaString: String) = {
    /*
    root
     |-- year: IntegerType
     |-- movies_count: LongType
     */
    val pattern = "\\|--(.*):".r()
    println(pattern.findAllMatchIn(schemaString).map(_.group(1).trim).mkString(" | "))
  }

  private def footer = {
    println()
    println("*********************************************************************************************************")
    println()
  }
}
