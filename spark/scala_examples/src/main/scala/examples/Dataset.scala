package examples

import model.MovieReview
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 *  A class for initializing a Spark Session and creating a Spark dataframe from
 *  the u.data file.
 */
case class DataFrameAPI(url : String, colNames : List[String] = List())

object MovieReviewsTyped {

  def run(): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val movieReviews = spark
      .read
      .schema(Encoders.product[MovieReview].schema)
      .option("delimiter", "\t")
      .csv("u.data")
      .as[MovieReview]

    movieReviews.show()

    spark.stop()
  }

}

