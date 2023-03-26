import os
import json
import time
import datetime
import requests
import pandas as pd
from threading import Thread


def get_products(category, page):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
        "origin": "https://tiki.vn",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    }

    params = {"category": category, "page": page, "limit": settings["page_limit"]}

    s_time = time.time()
    while time.time() - s_time < settings["waiting_time"]:
        try:
            res = requests.get(
                "https://tiki.vn/api/personalish/v1/blocks/listings",
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
            category = categories.pop(0)
        except:
            return

        page = 0
        while True:
            page += 1
            try:
                page_products = get_products(category["id"], page)
                print("\n" + "\t".join([f"{x}" for x in [f"category={category['id']}", f"page={page}", f"{len(page_products)} products"]]))
                for product in page_products:
                    if product["id"] not in products:
                        products[product["id"]] = True
                        print(f"\nCollected {len(products)} products: {product}")
                        with open(f"{os.getcwd()}/output/{len(products)//1000+1}.txt", "a", encoding="utf-8-sig") as f:
                            f.write(json.dumps(product) + "\n")
                if len(page_products) != settings["page_limit"]:
                    break
            except Exception as e:
                print(e)


global_time = time.time()
try:
    # settings.json
    print("=" * 100)
    print("settings: ")
    with open("settings.json", "r", encoding="utf-8") as f:
        settings = json.load(f)
    for id_key, key in enumerate(settings):
        print(f"{id_key+1}\t{key}: {settings[key]}")

    # import categories
    print("=" * 100)
    print("categories: ")
    df = pd.read_excel(settings["categories_file"])
    categories = df.to_dict("records")
    print(f"{len(categories)} categories")

    # initialize products
    products = {}

    # threads
    print("=" * 100)
    threads = []
    for i in range(settings["number_of_threads"]):
        threads.append(Thread(target=thread_flow))
        threads[-1].start()
    for thread in threads:
        thread.join()

    print("=" * 100)
    print("Exporting products")

except Exception as e:
    print(e)

print(f"Total time: {round(time.time() - global_time)} seconds")
print(f"FINISH!!!")
time.sleep(36000)
