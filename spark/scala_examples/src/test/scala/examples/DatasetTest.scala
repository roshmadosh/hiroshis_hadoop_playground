package examples

import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert._
import model.MovieReview
import org.apache.spark.sql.{Dataset, SparkSession}

object DatasetTest {
  private var spark: SparkSession = _
  private var ratingsDS: Dataset[MovieReview] = _
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
      .appName("Spark Dataset Test")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val ratingsRDD = sparkSession
      .sparkContext
      .parallelize(reviews)

    spark.createDataset(ratingsRDD)
  }

  @AfterClass
  def tearDown(): Unit = {
    spark.stop()
  }
}

class DatasetTest {
  import DatasetTest._

  @Test
  def getMaxAvgRatingWithIDReturnsMaxWithValidInput(): Unit = {
    val expected = (0, 4.5)
    val actual = DatasetExample.getMaxAvgRatingWithID(ratingsDS, 1)
    assertEquals(expected, actual)
  }
}
