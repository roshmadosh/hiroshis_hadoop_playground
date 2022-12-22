package examples

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 *  A class for initializing a Spark Session and creating a Spark dataframe from
 *  the u.data file.
 */
case class SparkDataFrame(url : String, colNames : List[String] = List()) {

  val spark = startSparkSession()
  val df = constructDataFrame()

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

  private def constructDataFrame() : DataFrame = {
    // hardcoded column-naming
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