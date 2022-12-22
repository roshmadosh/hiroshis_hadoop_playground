import examples.{Simple, SparkDataFrame}

object Main {
  def main(args : Array[String]): Unit = {
    val df_object = SparkDataFrame("u.data")
    val (spark, df) = df_object.initialize()

    val count = df.count()
    val filteredCount = df.filter("rating > 4").count()

    println(s"Count BEFORE filter $count")
    println(s"Count AFTER filter $filteredCount")

    spark.stop()
  }
}