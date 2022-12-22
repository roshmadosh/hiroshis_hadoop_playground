import org.apache.spark.{SparkContext, SparkConf}

object MyExample {

  def main(args : Array[String]): Unit = {

    // setMaster() is set to 'local' but would normally pass a URL to your data store (e.g. HDFS)
    val conf = new SparkConf().setAppName("My First Spark App").setMaster("local")
    val sc = new SparkContext(conf)

    // Just to make the output more visible
    sc.setLogLevel("error")

    // Upload data
    val lines = sc.textFile("data.txt")

    // Map-reduce in Scala!
    val charCount = lines.map(_.length).reduce(_+_)

    println(s"The number of characters in our data file is $charCount")

    // Stopping the SparkContext instance is recommended by the docs
    sc.stop()
  }
}