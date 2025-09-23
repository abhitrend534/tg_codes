from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_shein_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Perform bulk update
result = collection.update_many(
    {"availability": "no_stock"},  # Filter for documents with null availability
    {"$set": {"availability": "out_of_stock"}}  # Set new value
)

print(f'{result.modified_count} documents updated in {collection_name}.')

# Close the connection
client.close()
