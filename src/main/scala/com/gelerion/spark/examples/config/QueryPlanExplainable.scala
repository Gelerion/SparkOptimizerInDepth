package com.gelerion.spark.examples.config

import java.lang.reflect.Field

import org.apache.spark.sql.{SQLContext, SchemaRDD}

trait QueryPlanExplainable {
  val sqlContext: SQLContext

  def explain(sql: String): Unit = {
    val data = sqlContext.sql(sql)

    println("SQL:")
    println("----------------------------------------------------------------------------------")
    println(sql.trim)
    println()

    println("Logical Plan:")
    println("----------------------------------------------------------------------------------")
    println(logicalPlan(data))

    println("Analyzed Plan:")
    println("----------------------------------------------------------------------------------")
    println(data.queryExecution.analyzed)

    println("Physical Plan:")
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

  private def logicalPlan(data: SchemaRDD) = {
//    val mirror = universe.runtimeMirror(data.getClass.getClassLoader)
//    val instanceMirror = mirror.reflect(data)
    val field: Field = classOf[SchemaRDD].getDeclaredField("logicalPlan")
    field.setAccessible(true)
    field.get(data)
  }
}
