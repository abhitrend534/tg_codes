from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_lewkin_south_korea'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Update origin field from "Made in Korea" to "korea"
result = collection.update_many(
    {"origin": "Made in Korea"},
    {"$set": {"origin": 'korea'}}
)

print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# Close the connection
client.close()