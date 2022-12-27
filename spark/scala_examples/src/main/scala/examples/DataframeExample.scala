package examples

import org.apache.spark.sql.SparkSession
import model.MovieReview
object DataframeExample {

  def run(url : String ): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

    /* imports the companion object implicits from the spark library. I need this to call
    convert RDD to DF */
    import spark.implicits._
    val ratingsDF = spark.sparkContext
      .textFile(url)
      .map(line => {
        val attributes = line.split("\t").map(_.toInt)
        MovieReview(attributes(0), attributes(1), attributes(2), attributes(3))
      })
      .toDF()
  }
}


