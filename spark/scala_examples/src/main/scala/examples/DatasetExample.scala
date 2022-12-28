package examples

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import model.MovieReview
import org.apache.spark.sql.{Encoders, SparkSession}



/**
 *  A Spark Dataset definition for our movie reviews. A Spark Dataset is like a Spark Dataframe,
 *  but it's typed and will catch errors at compile-time.
 *
 *  The Dataset API is only available for Spark apps written in Java or Scala.
 */

object DatasetExample {

  def run(url : String): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val movieReviews = spark
      .read
      .option("delimiter", "\t")
      .csv(url)
      .as[MovieReview]


    val minRatings = 10
    getMaxAvgRatingWithID(movieReviews, minRatings)

    spark.stop()
  }

  def getMaxAvgRatingWithID(ds : Dataset[MovieReview], minRatings : Int): Unit = {
  /* Even though datasets are typed, as soon as we groupby or change the schema (e.g.
  * adding a column) it's no longer typed */
  }

}

