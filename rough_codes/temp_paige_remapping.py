from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_paige_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Find documents and update product_id and brand_product_id
for doc in collection.find({}, {"_id": 1, "images": 1}):
    images = doc.get("images")

    for i in images:
        i['image_style'] = 's0'

    # Update the document
    collection.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "images": images
            }
        }
    )

print(f'{collection_name} processing complete.')

# Close the connection
client.close()