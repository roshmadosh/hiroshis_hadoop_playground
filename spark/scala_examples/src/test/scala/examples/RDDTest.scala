package examples

import org.apache.spark.rdd.RDD
import org.junit.{After, AfterClass, Before, BeforeClass, Test}
import org.junit.Assert._
import org.apache.spark.{SparkConf, SparkContext}


// Arrange
object RDDTest {
  private var sc: SparkContext = _
  private var reviewsRDD: RDD[String] = _
  private val reviews = Array(
    "0\t0\t5\t100",
    "1\t0\t4\t100",
    "0\t1\t2\t100",
    "0\t1\t2\t100",
    "0\t1\t2\t100",
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
    val actual = RDD.getMaxAvgRatingWithID(reviewsRDD, 1)
    assertEquals(expected, actual)
  }

  @Test
  def getMaxAvgRatingWithIDIgnoresMoviesBelowMinReviews(): Unit = {
    val expected = (1, 2.0)
    val actual = RDD.getMaxAvgRatingWithID(reviewsRDD, 3)
    assertEquals(expected, actual)
  }
}
