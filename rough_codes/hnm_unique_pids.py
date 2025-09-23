import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

collection_name = 'crawler_sink_h&m_india'
collection = db[collection_name]

# Retrieve unique values for the key field
unique_pids = collection.distinct('product_id')

with open(f'hnm_unique_values.json', 'w') as f:
    json.dump(unique_pids, f, indent=4)

# Close the connection
client.close()