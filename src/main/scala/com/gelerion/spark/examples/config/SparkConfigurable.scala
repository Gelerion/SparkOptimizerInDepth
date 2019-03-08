package com.gelerion.spark.examples.config

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkConfigurable {
  val appName: String

  private lazy val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(appName)

  lazy val sc = new SparkContext(sparkConf)
  lazy val sqlContext = new SQLContext(sc)
}
