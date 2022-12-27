package examples

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import model.MovieReview
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 *  An example for Spark's DataFrame API
 */
object DataframeExample {

  def run(url : String ): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    /* Decreasing log output for readability. */
    spark.sparkContext.setLogLevel("warn")

    /* imports the companion object implicits from the spark library. I need this to call
    convert RDD to DF */
    import spark.implicits._

    val schema = new StructType()
      .add("userID", IntegerType)
      .add("movieID", IntegerType)
      .add("rating", IntegerType)
      .add("timestamp", IntegerType)

    val ratingsDF = spark.sparkContext
      .textFile(url)
      .map(line => {
        val attributes = line.split("\t").map(_.toInt)
        MovieReview(attributes(0), attributes(1), attributes(2), attributes(3))
      })
      .toDF()

    val minRatings = 10
    val maxAvg = getMaxAvgRatingWithID(ratingsDF, minRatings)

    println(s"The movie ID with the highest rating is ${maxAvg._1} with ${maxAvg._2}")
    spark.stop()
  }

  def getMaxAvgRatingWithID(df : DataFrame, minRatings: Int) = {
    df.groupBy("movieID")
      .agg( // this is convenient, not available on RDD
        avg("rating"),
        count("movieID")
      )
      .where(col("count(movieID)") >= minRatings)
      .map(row => // Encoders provided explicitly, could have also imported spark.implicit._
        (row.getAs[Int]("movieID"), row.getAs[Double]("avg(rating)")))(Encoders.tuple(Encoders.scalaInt, Encoders.scalaDouble))
      .collect // aggregate distributed data
      .toList
      .maxBy(_._2)
  }
}




