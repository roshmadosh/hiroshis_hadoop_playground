package examples

import org.junit.Test
import org.junit.Assert._
import examples.Basic
import model.MovieReview

class BasicTest {

  val reviews = Array(
    MovieReview(0, 1, 3, 100),
    MovieReview(0, 2, 3, 100),
    MovieReview(1, 1, 4, 100),
    MovieReview(2, 1, 5, 100),
    MovieReview(0, 2, 3, 100),
    MovieReview(0, 2, 3, 100),
    MovieReview(0, 2, 3, 100),
    MovieReview(0, 2, 3, 100),

  )
  @Test
  def getMaxAvgRatingReturnsMaxAvgWithID() : Unit = {
    val expected = (1, 4.0)
    val actual = Basic.getMaxAvgRating(reviews, 1)
    assertEquals(expected, actual)
  }

  @Test
  def getMaxAvgRatingMinReviewsParameterWorks() : Unit = {
    val expected = (2, 3.0)
    val actual = Basic.getMaxAvgRating(reviews, 4)
    assertEquals(expected, actual)
  }
}
