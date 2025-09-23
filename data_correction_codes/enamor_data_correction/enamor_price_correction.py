from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_enamor_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Update all documents where launch_price == None, set launch_price = price
result = collection.update_many(
    {"launch_price": 0, "price": {"$exists": True}},
    [
        {"$set": {"launch_price": "$price"}}
    ]
)

print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# Close the connection
client.close()