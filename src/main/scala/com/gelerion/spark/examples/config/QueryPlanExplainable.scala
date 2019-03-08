package com.gelerion.spark.examples.config

import org.apache.spark.sql.SQLContext

trait QueryPlanExplainable {
  val sqlContext: SQLContext

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
