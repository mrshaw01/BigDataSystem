import time
import json
import kafka
import happybase

def get_products(page_id):
    with open(f"./output/{page_id}.txt", "r") as f:
        lines = f.readlines()
        products = [json.loads(line.encode().decode("utf-8-sig")) for line in lines if len(line.strip()) != 0]
        return products

def consumer_thread(products):
    connection = happybase.Connection("node-master", 9090)
    products_table = connection.table("products_table")
    for message in products:
        item = message
        key = str(item["id"])
        data = {
            "product_info:json_string": json.dumps(item).encode("utf-8-sig"),
        }
        print(f"put {key} {data}")
        products_table.put(key, data)
    connection.close()


global_time = time.time()
for page in range(1, 1676):
    try:
        products = get_products(page)
        consumer_thread(products)
    except Exception as e:
        print(e)
print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
