import time
import json
import kafka
import happybase

global_time = time.time()

connection = happybase.Connection("node-master", 9090)
products_table = connection.table("products_table")
i=0
for key, data in products_table.scan():
	i += 1
	print(f"time = {round(time.time() - global_time)}, i={i}, key={key}, data={len(data)}")
	print(json.loads(data[b'product_info:json_string'].decode("utf-8-sig")))
	if i == 50000:
		break

connection.close()


print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
