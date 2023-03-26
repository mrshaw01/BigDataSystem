import time
import json
import kafka
import happybase
import pandas as pd


def get_categories():
    df = pd.read_excel("tiki-categories-2023-02-13-03-08-46.xlsx")
    categories = df.to_dict("records")
    return categories


def producer_thread():
    producer = kafka.KafkaProducer(
        bootstrap_servers=["node-master:9092", "node1:9092", "node2:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8-sig"),
    )
    for item in categories:
        producer.send("categories", item)
    producer.flush()


def consumer_thread():
    consumer = kafka.KafkaConsumer(
        value_deserializer=lambda x: json.loads(x.decode("utf-8-sig")),
    )
    connection = happybase.Connection("node-master", 9090)
    categories_table = connection.table("categories_table")
    for message in consumer:
        item = message.value
        key = str(item["id"])
        data = {
            "category_info:parent_id": str(item["parent_id"]),
            "category_info:name": item["name"],
            "category_info:level": str(item["level"]),
        }
        print(f"put {key} {data}")
        categories_table.put(key, data)
    connection.close()


global_time = time.time()
try:
    categories = get_categories()
    producer_thread()
    consumer_thread()
except Exception as e:
    print(e)

print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
