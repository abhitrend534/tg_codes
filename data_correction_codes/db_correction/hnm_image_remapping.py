import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'melody'
collection_name = 'crawler_sink_h&m_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Load mapping from file
with open('hnm_unique_color_images_updated.json', 'r') as file:
    unique_image_urls = json.load(file)

total = len(unique_image_urls)
print(f"Starting update for {total} color_id entries...\n")

# Update and log progress
for idx, (color_id, images) in enumerate(unique_image_urls.items(), start=1):
    result = collection.update_many(
        {'color_id': color_id},
        {'$set': {'images': images}}
    )
    print(f"[{idx}/{total}] Updated color_id '{color_id}': {result.modified_count} documents modified.")

# Close the connection
client.close()
