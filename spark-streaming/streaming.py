from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import happybase

def process_product(product_id):
    connection = happybase.Connection("node-master", 9090)
    table = connection.table('products_table')
    row = table.row(product_id)
    relevant_products = row[b'relevant_products:data']
    product_info = row[b'product_info']
    with open('relevant_products.txt', 'a') as f:
        f.write(relevant_products.decode('utf-8') + '\n')
    table.close()
    connection.close()

if __name__ == '__main__':
    conf = SparkConf().setAppName('ProductDataExtractor')
    ssc = StreamingContext(conf, 5)
    
    kafka_params = {'metadata.broker.list': 'node-master:9092,node1:9092,node2:9092'}
    topics = ['product_ids_topic']
    
    product_ids = KafkaUtils.createDirectStream(ssc, topics, kafka_params).map(lambda x: x[1])
    
    product_ids.foreachRDD(lambda rdd: rdd.foreach(process_product))
    
    ssc.start()
    ssc.awaitTermination()
