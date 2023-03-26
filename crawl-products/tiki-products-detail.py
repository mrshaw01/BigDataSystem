import time
import os
import json

count = 0
s_time = time.time()
for i in range(1, 1600):
	with open(f"output/{i}.txt", "r", encoding="utf-8-sig") as f:
		lines = f.readlines()
		for line in lines:
			if len(line.strip()) == 0:
				continue
			line = line.encode()
			line = line.decode("utf-8-sig")
			a = json.loads(line)
			count += len(f"{a}".split())
			print(f"Time={round(time.time()-s_time)}s, File={i}, total={count}")
