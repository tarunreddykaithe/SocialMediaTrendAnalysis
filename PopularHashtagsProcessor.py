import sys
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext, Row
import json,requests

def sumup_tags_counts(new_values, total_sum):
    return (total_sum or 0) + sum(new_values)

	 
def return_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

	 
def stream_dataframe_to_flask(df):
    top_tags = [str(t.tag) for t in df.select("tag").collect()]
    tags_count = [p.counts for p in df.select("counts").collect()]
    url = 'http://sandbox-hdp.hortonworks.com:5050/updateData'
    request_data = {'words': str(top_tags), 'counts': str(tags_count)}
    response = requests.post(url, data=request_data)

def process_rdd(time, rdd):
    print("------------- %s --------------" % str(time))
    try:
		# Use sql table to sort hashtags by count
        sql_context_instance = return_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(tag=w[0], counts=w[1]))
        tags_counts_df = sql_context_instance.createDataFrame(row_rdd)
        tags_counts_df.registerTempTable("tag_with_counts")
        selected_tags_counts_df = sql_context_instance.sql("select tag, counts from tag_with_counts order by counts desc limit 8")
        selected_tags_counts_df.show()
        #stream_dataframe_to_flask(selected_tags_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
		
 


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: PopularHashtags.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PopularHashTags")
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint("PopularHashTags-Checkpoint")

    zkQuorum, topic = sys.argv[1:]
    twitterKafkkaStream = KafkaUtils.createStream(ssc, zkQuorum, "Popular-Hashtags", {topic: 1}, {"auto.offset.reset": "largest"})
    words = twitterKafkkaStream.flatMap(lambda line: line.split(" "))
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    # hashtags = words.filter(lambda w: '@' in w).map(lambda x: (x, 1)) # process email count in tweets
    tags_totals = hashtags.updateStateByKey(sumup_tags_counts)  # Update checkpoint file with newest <key = hashtag, value = count> pairs
    tags_totals.foreachRDD(process_rdd)
    
    ssc.start()
    ssc.awaitTermination()