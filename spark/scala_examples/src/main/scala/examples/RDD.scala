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
    val results = getMaxAvgRatingWithID(lines)

    println(s"The movie with the highest avg rating is ${results._1} with a rating of ${results._2}")
    // Stopping the SparkContext instance is recommended by the docs
    sc.stop()
  }

  def getMaxAvgRatingWithID(rdd : RDD[String]): (Int, Double) = {

    val zero = (0, 0.0)
    val seqOp = (acc: (Int, Double), ele: (Int, Iterable[Array[String]])) => {
      val (maxID, maxAvg) = acc
      val (movieID, reviews) = ele

      val ratings = reviews.map(_.apply(2).toDouble)
      val avgRating = ratings.sum / ratings.size

      if (maxAvg >= avgRating) maxID -> maxAvg else movieID -> avgRating
    }

    val combOp = (pair1 : (Int, Double), pair2 : (Int, Double)) => {
      val (id1, rating1) = pair1
      val (id2, rating2) = pair2

      if (rating1 >= rating2) id1 -> rating1 else id2 -> rating2
    }

    /* Methods that generate an RDD from a file return an RDD[String] */
    rdd.map(_.split('\t'))
      /* Can't use a case class to establish a "schema" unless performing an extra map. */
      .groupBy(_.apply(1).toInt)
      /* The 'fold' method in Spark */
      .aggregate(zero) (seqOp, combOp)
  }

}