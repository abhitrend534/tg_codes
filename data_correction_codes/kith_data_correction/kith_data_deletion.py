import json
from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_kith_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Prepare bulk update operations
operations = []

prices = [5900, 6400, 13700, 15500, 19100]

for price in prices:
    collection.delete_many({'price': price})
    print(f"Deleted documents with price: {price}")

# Close the connection
client.close()