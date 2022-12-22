package examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 *  A "simple" example of performing data processing with Spark and Scala.
 *
 *  The run() function prints to STDOUT the average rating from the 'u.data' file.
 */
object Simple {

  def run(): Unit = {

    // setMaster() is set to 'local' but would normally pass a URL to your data store (e.g. HDFS)
    val conf = new SparkConf().setAppName("My First Spark App").setMaster("local")
    val sc = new SparkContext(conf)

    // Just to make the output more visible
    sc.setLogLevel("error")

    // Upload data
    val lines = sc.textFile("u.data")

    // Map-reduce in Scala!
    val charCount = lines
      .map(_.split("\t"))
      .map(_.apply(2))
      .map(_.toFloat)
      .reduce(_+_)

    val avg = charCount/lines.count()

    println("The average rating in the u.data file is %.2f".format(avg))

    // Stopping the SparkContext instance is recommended by the docs
    sc.stop()
  }
}