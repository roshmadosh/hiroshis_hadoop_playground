package examples

import org.apache.spark.rdd.RDD
import org.junit.{After, AfterClass, Before, BeforeClass, Test}
import org.junit.Assert._
import org.apache.spark.{SparkConf, SparkContext}
import model.MovieReview


// Arrange
object RDDTest {
  private var sc: SparkContext = _
  private var reviewsRDD: RDD[MovieReview] = _
  private val reviews = Array(
    MovieReview(0,0,5, 100),
    MovieReview(1,0,4, 100),
    MovieReview(0,1,2, 100),
    MovieReview(0,1,2, 100),
    MovieReview(0,1,2, 100),
  )

  @BeforeClass
  def initialize(): Unit = {
    val conf = new SparkConf().setAppName("test for RDD").setMaster("local")

    sc = new SparkContext(conf)
    reviewsRDD = sc.parallelize(reviews)
  }

  @AfterClass
  def teardown(): Unit = {
    sc.stop()
  }

}

class RDDTest {
  import RDDTest.{reviewsRDD}

  @Test
  def getMaxAvgRatingWithIDReturnsMaxWithValidInput(): Unit = {
    val expected = (0, 4.5)
    val actual = RDDExample.getMaxAvgRatingWithID(reviewsRDD, 1)
    assertEquals(expected, actual)
  }

  @Test
  def getMaxAvgRatingWithIDIgnoresMoviesBelowMinReviews(): Unit = {
    val expected = (1, 2.0)
    val actual = RDDExample.getMaxAvgRatingWithID(reviewsRDD, 3)
    assertEquals(expected, actual)
  }
}
