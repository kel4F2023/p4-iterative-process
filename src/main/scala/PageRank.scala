import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      graphTopicsPath: String,
      pageRankOutputPath: String,
      recsOutputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {

    val d = 0.85
    val sc = spark.sparkContext

    val graph = spark.read
      .option("delimiter", "\t")
      .csv(inputGraphPath)
      .toDF("follower_id", "user_id")
      .distinct()
      .select("user_id", "follower_id")
      .select(col("user_id").cast(LongType), col("follower_id").cast(LongType))
      .repartition(col("follower_id"))
      .cache()

    val users = graph
      .select("user_id")
      .union(graph.select("follower_id"))
      .distinct()
      .withColumnRenamed("user_id", "follower_id")
      .repartition(col("follower_id"))
      .cache()

    val n = users.count()
    val intercept = (1.0 - d)/ n

    val followsCount = graph
      .groupBy("follower_id")
      .count()
      .withColumnRenamed("count", "follows_count")
      .repartition(col("follower_id"))
      .cache()

    // Initialize the ranks of the users
    var ranks = users
      .withColumn("rank", lit(1.0 / n))
      .repartition(col("follower_id"))

    var rankPerFollow = graph
      .join(ranks, "follower_id")
      .withColumnRenamed("rank", "follower_rank")
      .repartition(col("follower_id"))

    var rankFollowersPerFollow = rankPerFollow
      .join(followsCount, "follower_id")
      .withColumnRenamed("follows_count", "follower_follows_count")
      .repartition(col("follower_id"))


     for (i <- 1 to iterations) {
       sc.setJobDescription(s"PageRank iteration ${i}")
      var contribes = rankFollowersPerFollow
        .groupBy("user_id")
        .agg(sum(col("follower_rank") / col("follower_follows_count")).as("contribution"))
        .withColumnRenamed("user_id", "follower_id")
        .repartition(col("follower_id"))
        .cache()

      var dangling =
        ( 1 - contribes.agg(sum(col("contribution")).as("total")).collect()(0).getAs[Double]("total")) / n

      var newRanks = ranks
        .join(contribes, Seq("follower_id"), "left")
        .withColumn(
          "contribution",
          when(col("contribution").isNull, lit(intercept) + lit(d) * lit(dangling))
            .otherwise(lit(intercept) + lit(d) * (col("contribution") + dangling)))
        .drop("rank")
        .withColumnRenamed("contribution", "rank")
        .repartition(col("follower_id"))

      rankPerFollow = graph
        .join(newRanks, "follower_id")
        .withColumnRenamed("rank", "follower_rank")
        .repartition(col("follower_id"))

      rankFollowersPerFollow = rankPerFollow
        .join(followsCount, "follower_id")
        .withColumnRenamed("follows_count", "follower_follows_count")
        .repartition(col("follower_id"))

      ranks = newRanks
       sc.setJobDescription(null)
     }

    sc.setJobDescription("PageRank saving results")
    ranks
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(pageRankOutputPath)
    sc.setJobDescription(null)

  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val graphTopics = args(1)
    val pageRankOutputPath = args(2)
    val recsOutputPath = args(3)

    calculatePageRank(inputGraph, graphTopics, pageRankOutputPath, recsOutputPath, PageRankIterations, spark)
  }
}
