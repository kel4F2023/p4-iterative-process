import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

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

    sc.setJobDescription("Loading data")

    val graph = spark.read
      .option("delimiter", "\t")
      .csv(inputGraphPath)
      .toDF("follower_id", "user_id")
      .distinct()
      .select("user_id", "follower_id")
      .select(col("user_id").cast(LongType), col("follower_id").cast(LongType))
      .repartition(col("follower_id"))
      .cache()

    var topics = spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .csv(graphTopicsPath)
      .toDF("follower_id", "games", "movies", "music")
      .withColumn("games",
        when(col("games") < 3.0, null)
          .otherwise(col("games").cast(DoubleType)))
      .withColumn("movies",
        when(col("movies") < 3.0, null)
          .otherwise(col("movies").cast(DoubleType)))
      .withColumn("music",
        when(col("music") < 3.0, null)
          .otherwise(col("music").cast(DoubleType)))
      .select(col("follower_id").cast(LongType), col("games"), col("movies"), col("music"))
      .repartition(col("follower_id"))

    val users = graph
      .select("user_id")
      .union(graph.select("follower_id"))
      .distinct()
      .withColumnRenamed("user_id", "follower_id")
      .repartition(col("follower_id"))
      .cache()

    var userTopics = users
      .join(topics, "follower_id")
      .repartition(col("follower_id"))

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

    // Initialize the recommendations of the users
    var recommend = userTopics
      .withColumn(
        "games",
        when(col("games").isNull, null)
          .otherwise(struct(col("games").cast(DoubleType).as("freq"), col("follower_id").cast(LongType).as("follower_id")))
      )
      .withColumn(
        "movies",
        when(col("movies").isNull, null)
          .otherwise(struct(col("movies").cast(DoubleType).as("freq"), col("follower_id").cast(LongType).as("follower_id")))
      )
      .withColumn(
        "music",
        when(col("music").isNull, null)
          .otherwise(struct(col("music").cast(DoubleType).as("freq"), col("follower_id").cast(LongType).as("follower_id")))
      )
      .repartition(col("follower_id"))

    val selfGraph = graph
      .union(graph.select("follower_id", "follower_id"))
      .repartition(col("follower_id"))
      .cache()



    for (i <- 1 to iterations) {
      sc.setJobDescription(s"PageRank iteration ${i}")
      var contribes = rankFollowersPerFollow
        .groupBy("user_id")
        .agg(sum(col("follower_rank") / col("follower_follows_count")).as("contribution"))
        .withColumnRenamed("user_id", "follower_id")
        .repartition(col("follower_id"))
        .cache()

      var dangling =
        ( 1 - contribes.agg(sum(col("contribution")).as("total")).first().getAs[Double]("total")) / n

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

      var newRecommend = selfGraph
        .join(recommend, "follower_id")
        .groupBy("user_id")
        .agg(
          max(col("games")).as("new_games"),
          max(col("movies")).as("new_movies"),
          max(col("music")).as("new_music")
        )
        .withColumnRenamed("user_id", "follower_id")
        .repartition(col("follower_id"))

      recommend = recommend
        .join(newRecommend, Seq("follower_id"), "left")
        .withColumn("games", when(col("games").isNull, null).when(col("new_games").isNull, col("games")).otherwise(col("new_games")))
        .withColumn("movies", when(col("movies").isNull, null).when(col("new_movies").isNull, col("movies")).otherwise(col("new_movies")))
        .withColumn("music", when(col("music").isNull, null).when(col("new_music").isNull, col("music")).otherwise(col("new_music")))
        .drop("new_games", "new_movies", "new_music")
        .repartition(col("follower_id"))

       sc.setJobDescription(null)
     }

    sc.setJobDescription("PageRank saving results")
    ranks
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(pageRankOutputPath)


    recommend
      .withColumn("recommendations", greatest(col("games"), col("movies"), col("music")))
      .withColumn("recommend_user_id", col("recommendations.follower_id"))
      .withColumn("recommend_freq", col("recommendations.freq"))
      .drop("games", "movies", "music", "recommendations")
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(recsOutputPath)

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
