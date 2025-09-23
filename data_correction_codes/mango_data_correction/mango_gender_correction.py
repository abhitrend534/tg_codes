from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_riverisland_uk'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Update all documents where age_range == '1y-17y'
result = collection.update_many(
    {"gender": "girls"},
    {"$set": {"gender": 'female'}}
)

print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# Close the connection
client.close()