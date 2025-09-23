import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

source_collection_name = 'crawler_sink_h&m_uk'
uk_pids = 'hnm_uk_brand_pids.json'

# Load product IDs from JSON file
with open(uk_pids, 'r') as json_file:
    hnm_uk_pids = json.load(json_file)

print(hnm_uk_pids.keys())

for brand in ['ARKET', 'COS', 'Weekday', '& Other Stories', 'Monki']:
    target_collection_name = f'crawler_sink_{brand.replace(" ", "_").lower()}_uk'
    source_collection = db[source_collection_name]
    target_collection = db[target_collection_name]

    for pid in hnm_uk_pids.get(brand, []):  # Using .get() to prevent KeyError
        entries = source_collection.find({'product_id': pid})
        entries_list = list(entries)  # Convert cursor to list
        
        if entries_list:  # Avoid inserting an empty list
            target_collection.insert_many(entries_list)