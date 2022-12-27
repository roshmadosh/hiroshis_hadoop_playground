import examples.{Basic, DataframeExample, RDDExample}

/**
 *  Driver class for testing examples.
 */
object Main {
  def main(args : Array[String]): Unit = {
    runDataFrameExample()
  }

  private def runBasicExample() : Unit = {
    Basic.run("u.data")
  }

  private def runRddExample() : Unit = {
    RDDExample.run("u.data")
  }
  private def runDataFrameExample() : Unit = {
    DataframeExample.run("u.data")
  }
}