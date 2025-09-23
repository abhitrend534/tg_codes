from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = 'v3_styles_skechers'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

key_field = 'url'

# Retrieve unique values for the key field
unique_urls = collection.distinct(key_field)

# Prepare bulk operations
operations = []

for url in unique_urls:
    updated_url = url.split('?')[0].strip()  # Remove query parameters

    if updated_url != url:
        print(f"Updating: {url} -> {updated_url}")
        operations.append(
            UpdateMany(
                {key_field: url},
                {"$set": {key_field: updated_url}}
            )
        )

# Execute bulk operation if there are any
if operations:
    result = collection.bulk_write(operations)
    print("Bulk update completed.")
    print(f"Modified count: {result.modified_count}")
else:
    print("No updates required.")

# Close the connection
client.close()