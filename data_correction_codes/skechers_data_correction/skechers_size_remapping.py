from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_skechers_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

sizes = collection.distinct('size_name')

# Prepare bulk operations
operations = []

for size in sizes:
    new_size = 'US ' + size

    operations.append(
        UpdateMany(
            {'size_name': size},
            {'$set': {'size_name': new_size}}
        )
    )

# Execute bulk operations if there are any
if operations:
    result = collection.bulk_write(operations)
    print(f"Bulk update result: {result.bulk_api_result}")

# Close the connection
client.close()