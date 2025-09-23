from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'v3_products_alo'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

key_field = 'title'

# Retrieve unique values for the key field
unique_titles = collection.distinct(key_field)

# Prepare bulk operations
operations = []

for title in unique_titles:
    cposition = title.rfind('-')
    if cposition == -1:
        continue  # Skip if '-' not found
    updated_title = title[:cposition].strip()

    if updated_title != title:
        print(f"Updating: {title} -> {updated_title}")
        operations.append(
            UpdateMany(
                {key_field: title},
                {"$set": {key_field: updated_title}}
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