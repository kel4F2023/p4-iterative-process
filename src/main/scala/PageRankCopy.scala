import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object PageRankCopy {

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

    val danglingUsers = users
      .withColumnRenamed("user_id", "user_id_follows_count")
      .repartition(col("user_id_follows_count"))
      .join(followsCount.repartition(col("user_id_follows_count")), col("user_id_follows_count"), "left_anti")

    var ranksPerFollow = graph
      .repartition(col("follower_id"))
      .join(ranks.repartition(col("follower_id")), col("follower_id"))
      .withColumnRenamed("rank", "follower_rank")
      .join(followsCount, graph("follower_id") === followsCount("user_id_follows_count"))
      .drop("user_id_follows_count")
      .withColumnRenamed("follows_count", "follower_follows_count")

    for (_ <- 1 to iterations) {
      var contribes = ranksPerFollow
        .groupBy("user_id")
        .agg(sum(col("follower_rank") / col("follower_follows_count")).as("contribution"))
        .withColumnRenamed("user_id", "user_id_rank")

      var dangling = danglingUsers
        .join(ranks, danglingUsers("user_id") === ranks("user_id_rank"))
        .agg(sum(col("rank")))
        .collect()(0)(0).asInstanceOf[Double] / n

      var newRanks = ranks
        .join(contribes, Seq("user_id_rank"), "left")
        .withColumn(
          "contribution",
          when(col("contribution").isNull, lit(intercept) + lit(d) * lit(dangling))
            .otherwise(lit(intercept) + lit(d) * (col("contribution") + dangling)))
        .drop("rank")
        .withColumnRenamed("contribution", "rank")

      ranksPerFollow = graph
        .join(newRanks, graph("follower_id") === newRanks("user_id_rank"))
        .drop("user_id_rank")
        .withColumnRenamed("rank", "follower_rank")
        .join(followsCount, graph("follower_id") === followsCount("user_id_follows_count"))
        .drop("user_id_follows_count")
        .withColumnRenamed("follows_count", "follower_follows_count")

      ranks = newRanks
    }

    ranks
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(pageRankOutputPath)

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
