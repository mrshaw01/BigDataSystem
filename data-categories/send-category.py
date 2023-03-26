import time
import json
import kafka
import happybase
import pandas as pd


def get_categories():
    df = pd.read_excel("tiki-categories-2023-02-13-03-08-46.xlsx")
    categories = df.to_dict("records")
    return categories


def consumer_thread(categories):
    connection = happybase.Connection("node-master", 9090)
    categories_table = connection.table("categories_table")
    print(categories_table)
    for item in categories:
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
    consumer_thread(categories)
except Exception as e:
    print(e)

print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
