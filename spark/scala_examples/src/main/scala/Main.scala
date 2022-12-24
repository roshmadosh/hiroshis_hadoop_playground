import examples.{DataFrameAPI, MovieReviewsTyped, RDD}

/**
 *  Driver class for testing examples.
 */
object Main {
  def main(args : Array[String]): Unit = {
    runDataFrameExample()
  }

  private def runRddExample() : Unit = {
    RDD.run()
  }
  private def runDataFrameExample() : Unit = {
    MovieReviewsTyped.run()
  }
}