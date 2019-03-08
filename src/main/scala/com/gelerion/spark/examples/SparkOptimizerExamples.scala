package com.gelerion.spark.examples

import com.gelerion.spark.examples.config.{QueryPlanExplainable, SparkConfigurable}
import com.gelerion.spark.examples.data.{Actor, Movie, Role, domain_data}

object SparkOptimizerExamples extends SparkConfigurable with QueryPlanExplainable {

  override val appName: String = "Optimizer"
  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
  import sqlContext._

  def main(args: Array[String]): Unit = {
    val moviesDb: Seq[Movie] = domain_data.movies
    val actorsDb: Seq[Actor] = domain_data.actors
    val rolesDb:  Seq[Role] = domain_data.roles

    val movies = sc.parallelize(moviesDb)
    val actors = sc.parallelize(actorsDb)
    val roles = sc.parallelize(rolesDb)

    movies.registerAsTable("movies")
    actors.registerAsTable("actors")
    roles.registerAsTable("roles")

    explain(
      """
        | SELECT movie.name, role.actorId
        | FROM movies movie
        | JOIN roles role ON movie.id = role.movieId
      """.stripMargin)


  }
}
