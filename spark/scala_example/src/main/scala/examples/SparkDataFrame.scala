package examples

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StructType}

case class SparkDataFrame(url : String, colNames : List[String] = List()) {

  val spark = startSparkSession()
  val df = constructDataFrame(url)

  def initialize() : (SparkSession, DataFrame) = {
    (spark, df)
  }
  private def startSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("My Spark DataFrame App")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    spark
  }

  private def constructDataFrame(url : String) : DataFrame = {
    val schema = new StructType()
      .add("userID", IntegerType, false)
      .add("movieID", IntegerType, false)
      .add("rating", IntegerType, false)
      .add("timestamp", IntegerType, false)

    val df = spark
      .read
      .options(Map("delimiter" -> "\t"))
      .schema(schema)
      .csv(url)

    df
  }

}