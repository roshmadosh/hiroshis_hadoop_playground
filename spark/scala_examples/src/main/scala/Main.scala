import examples.{Basic, MovieReviewsTyped, RDD}

/**
 *  Driver class for testing examples.
 */
object Main {
  def main(args : Array[String]): Unit = {
    runRddExample()
  }

  private def runBasicExample() : Unit = {
    Basic.run("u.data")
  }

  private def runRddExample() : Unit = {
    RDD.run("u.data")
  }
  private def runDataFrameExample() : Unit = {
    MovieReviewsTyped.run()
  }
}