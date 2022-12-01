package com.dmd.dfb.scala.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object DBConnection {
  def getSparkSession(dbName: String, tableName: String): DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("Data Feed Builder")
      .master("local")
      .getOrCreate()

    println("sparksession " + sparkSession)
    val jdbcUsername = <DB USER NAME>
    val jdbcPassword = <DB PWD>
    val jdbc_url = <s"jdbc:sqlserver://<DB HOST>:<PORT>;databaseName=${<DB NAME>}">
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    val sqlTableDF = sparkSession.read.jdbc(jdbc_url, tableName, connectionProperties)
    sqlTableDF
  }
}
