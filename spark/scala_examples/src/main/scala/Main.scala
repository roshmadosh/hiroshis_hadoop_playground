import examples.{Basic, MovieReviewsTyped, RDD}

/**
 *  Driver class for testing examples.
 */
object Main {
  def main(args : Array[String]): Unit = {
    runBasicExample()
  }

  private def runBasicExample() : Unit = {
    Basic.run("u.data")
  }

  private def runRddExample() : Unit = {
    RDD.run()
  }
  private def runDataFrameExample() : Unit = {
    MovieReviewsTyped.run()
  }
}