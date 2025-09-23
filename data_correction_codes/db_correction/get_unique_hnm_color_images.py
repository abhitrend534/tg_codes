import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_h&m_uk'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Aggregation to group by color_id and take first images entry
pipeline = [
    {
        "$match": {"images": {"$exists": True}}  # Optional: filter only docs with images
    },
    {
        "$group": {
            "_id": "$color_id",
            "images": {"$first": "$images"}
        }
    }
]

results = collection.aggregate(pipeline)

# Convert to dictionary
color_images_map = {doc['_id']: doc['images'] for doc in results if doc['_id'] is not None}

# Save to JSON
with open('hnm_unique_color_images_2.json', 'w') as json_file:
    json.dump(color_images_map, json_file, indent=2)

# Close the connection
client.close()