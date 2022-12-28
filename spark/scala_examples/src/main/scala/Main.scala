import examples.{Basic, DataframeExample, DatasetExample, RDDExample}

/**
 *  Driver class for testing examples.
 */
object Main {
  def main(args : Array[String]): Unit = {
    runDatasetExample()
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

  private def runDatasetExample() : Unit = {
    DatasetExample.run("u.data")
  }
}