#!/bin/bash
###### TEMPLATE run.sh ######
###### YOU NEED TO UNCOMMENT THE FOLLOWING LINE AND INSERT YOUR OWN PARAMETERS ######

spark-submit --conf spark.sql.adaptive.enabled=true --conf spark.sql.autoBroadcastJoinThreshold=20  --conf spark.driver.memory=10g --conf spark.executor.cores=6 --conf spark.default.parallelism=30 --num-executors=5	--executor-memory 10g --class PageRank target/project_spark.jar wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph-Topics wasbs:///pagerank-output wasbs:///recs-output

# For more information about tuning the Spark configurations, refer: https://spark.apache.org/docs/2.3.0/configuration.html
