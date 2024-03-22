import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FollowerDF {

  /**
   * This function should first read the graph located at the input path, it should compute the
   * follower count, and save the top 100 users to the output path in parquet format.
   *
   * It must be done using the DataFrame/Dataset API.
   *
   * It is NOT valid to do it with the RDD API, and convert the result to a DataFrame, nor to read
   * the graph as an RDD and convert it to a DataFrame.
   *
   * @param inputPath the path to the graph.
   * @param outputPath the output path.
   * @param spark the spark session.
   */
  def computeFollowerCountDF(inputPath: String, outputPath: String, spark: SparkSession): Unit = {
    val df = spark.read.option("delimiter", "\t")
      .csv(inputPath)
      .toDF("follower", "followee")
      .distinct()

    val top = df.groupBy("followee")
      .agg(count("follower").alias("count"))
      .orderBy(desc("count")).limit(100)

    top
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .parquet(outputPath)
  }

  /**
   * @param args it should be called with two arguments, the input path, and the output path.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val followerDFOutputPath = args(1)

    computeFollowerCountDF(inputGraph, followerDFOutputPath, spark)
  }

}
