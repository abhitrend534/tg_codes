from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"   # <-- update with your actual URI
database_name = 'tg_analytics'
collection_name = 'crawler_sink_underarmour_uk'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Update all documents where age_range == '1y-17y'
result = collection.update_many(
    {"age_range": "1y-17y"},
    {"$set": {"age_range": ["1y", "17y"]}}
)

print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# Close the connection
client.close()