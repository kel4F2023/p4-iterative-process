#!/bin/bash
###### TEMPLATE run.sh ######
###### YOU NEED TO UNCOMMENT THE FOLLOWING LINE AND INSERT YOUR OWN PARAMETERS ######

spark-submit	--conf spark.dynamicAllocation.enabled=true --conf spark.sql.adaptive.enabled=true --conf spark.sql.autoBroadcastJoinThreshold=200 --conf spark.driver.memory=5g --conf spark.executor.cores=7 --conf spark.default.parallelism=35 --num-executors=5	--executor-memory 15g --class PageRank target/project_spark.jar wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph wasb://datasets@clouddeveloper.blob.core.windows.net/iterative-processing/Graph-Topics wasbs:///pagerank-output wasbs:///recs-output

# For more information about tuning the Spark configurations, refer: https://spark.apache.org/docs/2.3.0/configuration.html
