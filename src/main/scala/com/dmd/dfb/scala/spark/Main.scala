package com.dmd.dfb.scala.spark

import org.apache.spark.sql.SparkSession

object Main extends App{

    val clientTable = DBConnection.getSparkSession("<DB NAME>","<DB TABLE>")
    println("clientTable ---> " + clientTable.select("<COLUMN NAME>","<COLUMN NAME>").show())

}
