import org.apache.spark.SparkContext

object FollowerRDD {

  /**
   * This function should first read the graph located at the input path, it should compute the
   * follower count, and save the top 100 users to the output path with userID and
   * count **tab separated**.
   *
   * It must be done using the RDD API.
   *
   * @param inputPath the path to the graph.
   * @param outputPath the output path.
   * @param sc the SparkContext.
   */
  def computeFollowerCountRDD(inputPath: String, outputPath: String, sc: SparkContext): Unit = {
    val graphRDD = sc.textFile(inputPath)
      .map(_.split("\t").map(_.toInt))
      .map(a => (a(0), a(1)))
      .distinct()

    val top = graphRDD
      .map(a => (a._2, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, ascending = false)
      .map{ case (user, count) => s"$user\t$count" }
      .take(100)

    val first100RDD = sc.parallelize(top)

    first100RDD.saveAsTextFile(outputPath)
  }

  /**
   * @param args it should be called with two arguments, the input path, and the output path.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputGraph = args(0)
    val followerRDDOutputPath = args(1)

    computeFollowerCountRDD(inputGraph, followerRDDOutputPath, sc)
  }
}
