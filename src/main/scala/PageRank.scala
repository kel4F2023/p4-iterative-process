import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}

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

    val schema = new StructType()
      .add("follower", LongType)
      .add("followee", LongType)

    val links = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv(inputGraphPath)
      .repartition(col("follower"))
      .cache()

    val users = links
      .select(col("follower").as("user_id"))
      .union(links.select(col("followee").as("user_id")))
      .distinct()
      .repartition(col("user_id"))
      .cache()

    val n = users.count()
    val intercept = 0.15 / n

    val userFollows = links.groupBy("follower")
      .agg(collect_list("followee").as("followees"))
      .repartition(col("follower"))
      .cache()

    var ranks = userFollows
      .select(col("followee").as("url"), lit(1.0 / n).as("rank"))
      .distinct()
      .repartition(col("url"))

    ranks.show()

    for (i <- 1 to 1) {

      sc.setJobDescription(s"PageRank iteration ${i}")

      val contrib = userFollows.join(ranks, col("follower") === col("url"), "left")
        .withColumn("contrib", lit(col("rank") / size(col("followees"))))
        .withColumn("followee", explode(col("followees")))
        .select(col("followee"), col("contrib"))

      userFollows.show()

      val dangling =
        ( 1 - contrib.select(sum(col("contrib")).as("total")).first().getAs[Double]("total")) / n


      ranks = contrib.select(col("followee").as("url"), col("contrib"))
        .groupBy("url")
        .agg(sum(col("contrib")).as("total_contrib"))
        .withColumn("rank",
          lit(intercept) + lit(d) * (col("total_contrib") + lit(dangling))
        )
        .select(col("url"), col("rank"))
        .repartition(col("url"))






//      var newRecommend = selfGraph
//        .join(userRecommends, "follower_id")
//        .groupBy("user_id")
//        .agg(
//          max(col("games")).as("new_games"),
//          max(col("movies")).as("new_movies"),
//          max(col("music")).as("new_music")
//        )
//        .withColumnRenamed("user_id", "follower_id")
//        .repartition(col("follower_id"))
//
//      userRecommends = userRecommends
//        .join(newRecommend, Seq("follower_id"), "left")
//        .withColumn("games", when(col("games").isNull, null).when(col("new_games").isNull, col("games")).otherwise(col("new_games")))
//        .withColumn("movies", when(col("movies").isNull, null).when(col("new_movies").isNull, col("movies")).otherwise(col("new_movies")))
//        .withColumn("music", when(col("music").isNull, null).when(col("new_music").isNull, col("music")).otherwise(col("new_music")))
//        .drop("new_games", "new_movies", "new_music")
//        .repartition(col("follower_id"))
//        .cache()

       sc.setJobDescription(null)
     }

    sc.setJobDescription("PageRank saving results")
//    ranks.show()
//    ranks
//      .write
//      .option("delimiter", "\t")
//      .option("header", "false")
//      .csv(pageRankOutputPath)


//    userRecommends
//      .withColumn("recommendations", greatest(col("games"), col("movies"), col("music")))
//      .withColumn("recommend_user_id", col("recommendations.follower_id"))
//      .withColumn("recommend_freq", col("recommendations.freq"))
//      .drop("games", "movies", "music", "recommendations")
//      .write
//      .option("delimiter", "\t")
//      .option("header", "false")
//      .csv(recsOutputPath)

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
