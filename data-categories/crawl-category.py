import json
import time
import datetime
import requests
import pandas as pd
from threading import Thread


def get_categories(parent_id):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
        "origin": "https://tiki.vn",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    }

    params = {
        "parent_id": parent_id,
    }

    s_time = time.time()
    while time.time() - s_time < settings["waiting_time"]:
        try:
            res = requests.get(
                "https://api.tiki.vn/v2/categories",
                params=params,
                headers=headers,
                proxies=settings["proxies"],
            )
            res.json()
            break
        except:
            pass
    try:
        return res.json()["data"]
    except:
        raise Exception(f"{res.text}")
    return res.json()


def thread_flow():
    while True:
        try:
            category_id = parent_categories.pop(0)
            processing_categories.add(category_id)
        except:
            if len(processing_categories) == 0 and len(parent_categories) == 0:
                return
            time.sleep(1)
            continue

        try:
            res = get_categories(parent_id=category_id)
            for category in res:
                if category["id"] not in categories:
                    categories[category["id"]] = category
                    parent_categories.append(category["id"])
                    print(f"\nCollected {len(categories.values())} categories: {category}")
        except:
            pass
        processing_categories.remove(category_id)


global_time = time.time()
try:
    # settings.json
    print("=" * 100)
    print("settings: ")
    with open("settings.json", "r", encoding="utf-8") as f:
        settings = json.load(f)
    for id_key, key in enumerate(settings):
        print(f"{id_key+1}\t{key}: {settings[key]}")

    # initialize categories dictionary
    categories = {}

    # parent categories for dfs purpose
    parent_categories = ["2"]
    processing_categories = set({})

    print("=" * 100)

    # threads
    threads = []
    for i in range(settings["number_of_threads"]):
        threads.append(Thread(target=thread_flow))
        threads[-1].start()
    for thread in threads:
        thread.join()

    print("=" * 100)
    print("Exporting categories")

    # export categories
    df = pd.DataFrame(categories.values())
    df.sort_values(by=["id"], inplace=True)
    df.to_excel(
        f"tiki-categories-{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.xlsx",
        index=False,
    )
except Exception as e:
    print(e)

print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
