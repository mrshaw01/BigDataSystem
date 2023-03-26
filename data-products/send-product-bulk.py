import time
import json
import kafka
import happybase
import threading
from threading import Thread

global_time = time.time()
def get_products(page_id):
    print(f"time={round(time.time()-global_time)}\tpage={page_id}")
    with open(f"./output/{page_id}.txt", "r") as f:
        lines = f.readlines()
        products = [json.loads(line.encode().decode("utf-8-sig")) for line in lines if len(line.strip()) != 0]
        return products

def consumer_thread(products):
    connection = happybase.Connection("node-master", 9090)
    products_table = connection.table("products_table")
    
    batch_size = 600
    batch = products_table.batch(batch_size=batch_size)
    
    i = 0
    for message in products:
        if i % 8 != 0:
            i += 1
            continue
        i += 1
        item = message
        key = str(item["id"])
        data = {
            "product_info:json_string": json.dumps(item).encode("utf-8-sig"),
        }
        batch.put(key, data)
        if len(batch._mutations) == batch_size:
            batch.send()
    if len(batch._mutations):
            batch.send()
    connection.close()


global_time = time.time()


def flow_thread():
    while True:
        try:
            page = pages.pop(0)
        except:
            return

        products = get_products(page)
        consumer_thread(products)


# pages
pages = list(range(1, 1676))

# start threads to load pages
threads = []
for i in range(20):
    threads.append(Thread(target=flow_thread))
    threads[-1].start()
for thread in threads:
    thread.join()

print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
