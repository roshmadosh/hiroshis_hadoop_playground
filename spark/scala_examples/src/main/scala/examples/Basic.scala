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

    /* Print the first five reviews */
    data.take(5) foreach println

    source.close()
  }

}
