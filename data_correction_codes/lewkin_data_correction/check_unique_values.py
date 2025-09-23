from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_lewkin_south_korea'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

field_to_lower_case = 'title'

# Use update_many with aggregation pipeline for bulk update (MongoDB 4.2+)
result = collection.update_many(
    {field_to_lower_case: {"$exists": True, "$type": "string"}},
    [
        {"$set": {field_to_lower_case: {"$toLower": f"${field_to_lower_case}"}}}
    ]
)

print(f"{result.modified_count} titles have been converted to lowercase.")            


# Close the connection
client.close()