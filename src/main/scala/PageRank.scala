import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
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

    LogManager.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

    val schema = new StructType()
      .add("follower", LongType)
      .add("followee", LongType)

    val topicSchema = new StructType()
      .add("url", LongType)
      .add("games", DoubleType)
      .add("movies", DoubleType)
      .add("music", DoubleType)

    val links = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv(inputGraphPath)
      .cache()

    val topics = spark.read
      .option("delimiter", "\t")
      .schema(topicSchema)
      .csv(graphTopicsPath)
      .cache()

    val users = links
      .select(col("follower").as("user_id"))
      .union(links.select(col("followee").as("user_id")))
      .distinct()
      .cache()

    val n = users.count()
    val intercept = 0.15 / n

    val userFollows = links.groupBy("follower")
      .agg(collect_list("followee").as("followees"))
      .cache()

    var ranks = users
      .select(col("user_id").as("url"), lit(1.0 / n).as("rank"))
      .distinct()
      .repartition(col("url"))

    var vars = topics
      .join(ranks, "url")
      .withColumn("games_rec", when(topics("games") < 3.0, null).otherwise(array(topics("games").cast(DoubleType), topics("url"))))
      .withColumn("movies_rec", when(topics("movies") < 3.0, null).otherwise(array(topics("movies").cast(DoubleType), topics("url"))))
      .withColumn("music_rec", when(topics("music") < 3.0, null).otherwise(array(topics("music").cast(DoubleType), topics("url"))))
      .select("url", "games_rec", "movies_rec", "music_rec", "rank")

    val static = vars
      .withColumnRenamed("rank", "static_rank")
      .withColumnRenamed("games_rec", "static_games_rec")
      .withColumnRenamed("movies_rec", "static_movies_rec")
      .withColumnRenamed("music_rec", "static_music_rec")
      .cache()

    val window = Window.partitionBy("url")


    for (i <- 1 to iterations) {

      sc.setJobDescription(s"PageRank iteration ${i}")

      val startTime = System.nanoTime()

      val contrib = userFollows.join(vars, col("follower") === col("url"), "left")
        .withColumn("contrib", lit(col("rank") / size(col("followees"))))
        .withColumn("followee", explode_outer(concat(col("followees"), array(col("follower")))))
        .withColumn("contrib", when(col("followee") === col("follower"), lit(0.0)).otherwise(col("contrib")))
        .select(col("followee"), col("contrib"), col("games_rec"), col("movies_rec"), col("music_rec"))

      vars = contrib.select(col("followee").as("url"), col("contrib"), col("games_rec"), col("movies_rec"), col("music_rec"))
        .repartition(col("url"))
        .groupBy("url")
        .agg(
          sum(col("contrib")).as("total_contrib"),
          max(col("games_rec")).as("games_rec"),
          max(col("movies_rec")).as("movies_rec"),
          max(col("music_rec")).as("music_rec")
        )
        .withColumn("rank",
          lit(intercept) + lit(d) * (col("total_contrib") + (lit(1) - sum("total_contrib").over()) / lit(n))
        )
        .select(col("url"), col("rank"), col("games_rec"), col("movies_rec"), col("music_rec"))

      vars = vars.join(static, "url")
        .withColumn("games_rec", when(col("static_games_rec")(0).isNull, null).otherwise(col("games_rec")))
        .withColumn("movies_rec", when(col("static_movies_rec")(0).isNull, null).otherwise(col("movies_rec")))
        .withColumn("music_rec", when(col("static_music_rec")(0)isNull, null).otherwise(col("music_rec")))
        .select("url", "games_rec", "movies_rec", "music_rec", "rank")

       sc.setJobDescription(null)

      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9
      println(s"PageRank iteration ${i} took: $duration seconds")
     }

    sc.setJobDescription("PageRank saving results")

    var startTime = System.nanoTime()

    vars.cache()

    vars
      .withColumn("recs", greatest(col("games_rec"), col("movies_rec"), col("music_rec")))
      .select(col("url"), col("recs")(1).cast(LongType), col("recs")(0))
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(recsOutputPath)


    vars
      .select("url", "rank")
      .write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(pageRankOutputPath)

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9
    println(s"PageRank saving results took: $duration seconds")


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
