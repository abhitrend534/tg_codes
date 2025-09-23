import json
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_h&m_india'

# Load mapping from file
with open('hnm_unique_color_images_updated_1.json', 'r') as file:
    unique_image_urls = json.load(file)

total = len(unique_image_urls)
print(f"Starting update for {total} color_id entries...\n")

# MongoDB client needs to be shared across threads
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Update function for each thread
def update_color_id(color_id, images):
    result = collection.update_many(
        {'color_id': color_id},
        {'$set': {'images': images}}
    )
    return (color_id, result.modified_count)

# Use ThreadPoolExecutor for parallel processing
max_threads = 16  # You can tweak this based on your system
with ThreadPoolExecutor(max_workers=max_threads) as executor:
    futures = {
        executor.submit(update_color_id, color_id, images): color_id
        for color_id, images in unique_image_urls.items()
    }

    for idx, future in enumerate(as_completed(futures), start=1):
        color_id = futures[future]
        try:
            _, modified_count = future.result()
            print(f"[{idx}/{total}] Updated color_id '{color_id}': {modified_count} documents modified.")
        except Exception as e:
            print(f"[{idx}/{total}] Failed to update color_id '{color_id}': {e}")

# Close MongoDB connection
client.close()
