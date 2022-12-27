package examples

import model.MovieReview
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  An example for Spark's RDD API.
 */
object RDDExample {

  def run(url : String) : Unit = {

    /* setMaster() is set to 'local' but would normally pass a URL to your data store (e.g. HDFS) */
    val conf = new SparkConf().setAppName("Spark RDD in Action").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /* Just to make the output more visible */
    sc.setLogLevel("error")

    /* textFile method returns an RDD[String]. Map to RDD[MovieReview].
    Cache() allows for subsequent use of the same instance. */
    val lines = sc
      .textFile(url)
      .map(line => {
        val attributes = line.split("\t").map(_.toInt)
        MovieReview(attributes(0), attributes(1), attributes(2), attributes(3))
      })
      .cache()

    /* Invoke our function */
    val minReviews = 10
    val results = getMaxAvgRatingWithID(lines, minReviews)

    println(s"The movie with the highest avg rating is ${results._1} with a rating of ${results._2}")

    /* Stopping the SparkContext instance is recommended by the docs */
    sc.stop()
  }

  def getMaxAvgRatingWithID(rdd : RDD[MovieReview], minReviews: Int): (Int, Double) = {

    /* Starting values for the accumulator */
    val zero = (0, 0.0)

    /* Like a reducer function that can return any type */
    val seqOp = (acc: (Int, Double), ele: (Int, Iterable[MovieReview])) => {

      /* Extra step of destructuring arguments, can't define these in parameter definition. */
      val (maxID, maxAvg) = acc
      val (movieID, reviews) = ele

      /* Early termination if below minReviews */
      if (reviews.size < minReviews) maxID -> maxAvg

      else {
        val ratings = reviews.map(_.rating.toDouble)
        val avgRating = ratings.sum / ratings.size

        if (maxAvg >= avgRating) maxID -> maxAvg else movieID -> avgRating
      }

    }

    /* A function that defines how to combine or compare the results from seqOp. */
    val combOp = (pair1 : (Int, Double), pair2 : (Int, Double)) => {
      val (id1, rating1) = pair1
      val (id2, rating2) = pair2

      if (rating1 >= rating2) id1 -> rating1 else id2 -> rating2
    }

    /* Rows in our file are tab-delimited. */
    rdd.groupBy(_.movieID)
      .aggregate(zero) (seqOp, combOp)
  }

}