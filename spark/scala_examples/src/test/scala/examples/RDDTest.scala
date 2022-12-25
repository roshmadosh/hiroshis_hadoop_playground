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
    "0 0 5 100",
    "1 0 4 100",
    "0 1 2 100"
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
    val actual = RDD.getMaxAvgRatingWithID(reviewsRDD)
    assertEquals(expected, actual)
  }

}
