package examples

import model.MovieReview
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  Doing the same thing as we did in the Basic example, but using Spark's RDD.
 *
 */
object RDD {

  def run(url : String) : Unit = {

    // setMaster() is set to 'local' but would normally pass a URL to your data store (e.g. HDFS)
    val conf = new SparkConf().setAppName("Spark RDD in Action").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Just to make the output more visible
    sc.setLogLevel("error")

    // Upload data
    val lines = sc.textFile(url)
    lines.take(5) foreach println
//    val results = getMaxAvgRatingWithID(lines)

    // Stopping the SparkContext instance is recommended by the docs
    sc.stop()
  }

  def getMaxAvgRatingWithID(rdd : RDD[String]): (Int, Double) = {
    (0,0.0)
  }

}