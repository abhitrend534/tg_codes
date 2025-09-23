from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = 'tg_analytics'

geographies = ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa']

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

for geography in geographies:
    collection_name = f'crawler_sink_zara_{geography}_kids'
    collection = db[collection_name]

    # Find documents and update product_id and brand_product_id
    for doc in collection.find({}, {"_id": 1, "size_name": 1}):
        size = doc.get("size_name")
        if '(' not in size:
            collection.delete_one({"_id": doc["_id"]})
        else:
            size = size.split('(')[0].strip()
            if 'months' in size or 'years' in size:
                size = size
            else:
                collection.delete_one({"_id": doc["_id"]})

    print(f'{collection_name} processing complete.')

# Close the connection
client.close()