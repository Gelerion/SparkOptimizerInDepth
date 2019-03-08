package com.gelerion.spark.examples

import com.gelerion.spark.examples.config.{QueryPlanExplainable, SparkConfigurable}
import com.gelerion.spark.examples.data.{Actor, Movie, Role, domain_data}

object SparkAnalyzerExamples extends SparkConfigurable with QueryPlanExplainable {

  override val appName: String = "Analyzer"
  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
  import sqlContext._

  /*
    Analyzer is responsible for:
     1. ResolveReferences (UnresolvedAttribute)
     2. ResolveRelations (UnresolvedRelation, lookup from Catalog)
     3. StarExpansion (* to set of expressions)
     4. ResolveFunctions (UnresolvedFunction, lookup from FunctionsRegistry)
     5. GlobalAggregates
   */

  def main(args: Array[String]): Unit = {
    val moviesDb: Seq[Movie] = domain_data.movies
    val actorsDb: Seq[Actor] = domain_data.actors
    val rolesDb:  Seq[Role]  = domain_data.roles

    val movies = sc.parallelize(moviesDb)
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    movies.registerAsTable("movies")

    //initial plan derived via SqlParser
    explain("SELECT * FROM movies")
    //we can observe two

    //filter condition
    explain("SELECT name, year FROM movies WHERE year >= 2017")

    //complex sql
    explain(
      """
        |SELECT year, count(year) AS movies_count
        |FROM movies
        |WHERE name LIKE '%a%'
        |GROUP BY year
        |HAVING movies_count >= 1
        |ORDER BY movies_count DESC
        |LIMIT 5
      """.stripMargin)
  }
}
