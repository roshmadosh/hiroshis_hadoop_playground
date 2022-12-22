import examples.{Simple, SparkDataFrame}

/**
 *  Driver class for testing examples.
 */
object Main {
  def main(args : Array[String]): Unit = {
    runSimpleExample()
  }

  private def runSimpleExample() : Unit = {
    Simple.run()
  }
  private def runDataFrameExample() : Unit = {
    // get Spark Session and a DataFrame instance for our 'u.data' file
    val df_object = SparkDataFrame("u.data")
    val (spark, df) = df_object.initialize()

    // do some stuff with the DataFrame API
    val count = df.count()
    val filteredCount = df.filter("rating > 4").count()

    println(s"Count BEFORE filter $count")
    println(s"Count AFTER filter $filteredCount")

    // close the Spark Session
    spark.stop()
  }
}