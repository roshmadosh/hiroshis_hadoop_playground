package examples

/**
 *  Shows how to process data using vanilla Scala (without Spark)
 */
object Basic {
  import model.MovieReview

  def run(url : String) : Unit = {

    /* Upload data from source */
    val source = scala.io.Source.fromFile(url)
    val lines = source.getLines()

    /* Map each line to a MovieReview instance */
    val data = lines.map { line =>
      val features = line.split("\t")
      MovieReview(features(0).toInt, features(1).toInt, features(2).toInt, features(3).toInt)
    }.toArray

    /* Get movie ID with highest avg rating and at least 10 reviews */
    val minReviews = 10
    val (movieID, avg) = getMaxAvgRating(data, minReviews)
    println(s"Max average rating with at least $minReviews reviews is $avg by movie ID $movieID")

    source.close()
  }

  def getMaxAvgRating(data : Array[MovieReview], minReviews: Int) : (Int, Double) = {

    /* foldLeft is like reduce but the return type can be different. In this case it serves
    to prevent an extra traversal through the array by using a maxAvg accumulator */
    data.groupBy(_.movieID).foldLeft(-1 -> 0.0) {

      /* case statement here is syntactic sugar for variable destructuring within a higher
      order method (i.e. map, reduce, foldLeft, etc.) */
      case ((maxMovieID, maxRating), (movieID, reviews)) =>

        /* Skip movies with less than minReviews-many reviews */
        if (reviews.length < minReviews)
          maxMovieID -> maxRating

        /* Without else block, loop won't exit early even if the IF condition is met.
        Returning the above result just exists the foldLeft function. */
        else {
          val ratings = reviews.map(_.rating)
          val avgRating = ratings.sum.toDouble / ratings.length

          if (maxRating >= avgRating) maxMovieID -> maxRating else movieID -> avgRating
        }
    }

  }

}
