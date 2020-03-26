python PopularHashtagsStreamer.py
sleep 90
PYSPARK_PYTHON=python3.6 spark-submit
 --master yarn
 --deploy-mode client --conf "spark.dynamicAllocation.enabled=false"
 --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar
  PopularHashtagsProcessor.py sandbox-hdp.hortonworks.com:2181 PopularHashtags