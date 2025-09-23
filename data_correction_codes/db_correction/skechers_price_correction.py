from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_skechers_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

key_field_1 = 'price'
key_field_2 = 'launch_price'

# exchange price and  launch_price for each entry
unique_entries = collection.find({key_field_1: {"$exists": True}, key_field_2: {"$exists": True}})

for entry in unique_entries:  
    price = entry.get(key_field_1)
    launch_price = entry.get(key_field_2)

    if price is not None and launch_price is not None:
        print(f"Updating: {entry['_id']} - {key_field_1}: {price} <-> {key_field_2}: {launch_price}")
        collection.update_one(
            {'_id': entry['_id']},
            {"$set": {key_field_1: launch_price, key_field_2: price}}
        )

# Close the connection
client.close()