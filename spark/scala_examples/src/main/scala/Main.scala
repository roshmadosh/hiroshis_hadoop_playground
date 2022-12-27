import examples.{Basic, MovieReviewsTyped, RDDExample}

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
    RDDExample.run("u.data")
  }
  private def runDataFrameExample() : Unit = {
    MovieReviewsTyped.run()
  }
}