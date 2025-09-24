from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

countries = ['india', 'usa']
for country in countries:
    collection_name = f'crawler_sink_marknspencer_{country}'
    collection = db[collection_name]

    # Prepare bulk operations
    operations = []

    # replace '_' with '' in sku
    for doc in collection.find({"sku": {"$regex": "_"}}):
        new_sku = doc['sku'].replace('_', '').split('(')[0].strip().replace(' ', '_')
        operations.append(
            UpdateMany(
                {'_id': doc['_id']},
                {'$set': {'sku': new_sku}}
            )
        )

    # Execute bulk operations if there are any
    if operations:
        result = collection.bulk_write(operations)
        print(f"Bulk update result: {result.bulk_api_result}")

# Close the connection
client.close()