package examples

import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert._
import model.MovieReview
import org.apache.spark.sql.{SparkSession,DataFrame}

object DataframeTest {
  private var spark: SparkSession = _
  private var ratingsDF: DataFrame = _
  private val reviews = Array(
    MovieReview(0, 0, 5, 100),
    MovieReview(1, 0, 4, 100),
    MovieReview(0, 1, 2, 100),
    MovieReview(0, 1, 2, 100),
    MovieReview(0, 1, 2, 100),
  )

  @BeforeClass
  def initialize(): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Spark Dataframe Test")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    ratingsDF = sparkSession.sparkContext.parallelize(reviews).toDF()

    spark = sparkSession
  }

  @AfterClass
  def teardown(): Unit = {
    spark.stop()
  }

}

class DataframeTest {
  import DataframeTest._

  @Test
  def getMaxAvgRatingWithIDReturnsMaxWithValidInput(): Unit = {
    val expected = (0, 4.5)
    val actual = DataframeExample.getMaxAvgRatingWithID(ratingsDF, 1)
    assertEquals(expected, actual)
  }

  @Test
  def getMaxAvgRatingWithIDIgnoresBelowMinRatings(): Unit = {
    val expected = (1, 2.0)
    val actual = DataframeExample.getMaxAvgRatingWithID(ratingsDF, 3)
    assertEquals(expected, actual)
  }
}


