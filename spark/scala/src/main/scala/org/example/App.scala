package org.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object App {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("My First Spark App").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data.txt")
    val charCount = lines.map(_.length).reduce(_+_)

    println(charCount)
    sc.stop()
  }

}
