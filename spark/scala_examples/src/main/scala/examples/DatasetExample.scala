package examples


import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 *  A Spark Dataset definition for our movie reviews. A Spark Dataset is like a Spark Dataframe,
 *  but it's typed and will catch errors at compile-time.
 *
 *  The Dataset API is only available for Spark apps written in Java or Scala.
 */

object DatasetExample {
  import org.apache.spark.sql.{Encoders, SparkSession}
  import model.MovieReview

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

