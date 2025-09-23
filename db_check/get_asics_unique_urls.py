import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_asics_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]


sizes = collection.distinct('url', {'gender': 'unisex'})

with open('asics_unique_urls.json', 'w') as file:
    json.dump(sizes, file, indent=4)

# Close the connection
client.close()