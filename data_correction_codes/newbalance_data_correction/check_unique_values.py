from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = f'crawler_sink_newbalance_uae'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values and their types for the size_name field
unique_sizes = collection.distinct('size_name')
size_types = set(type(size).__name__ for size in unique_sizes)

print("Unique sizes:", unique_sizes)
print("Data types of sizes:", size_types)

'''
# --- NEW: count, sample and delete documents where size_name is a dict ---
# Count documents where size_name is stored as a document (MongoDB "object")
filter_obj_type = {'size_name': {'$type': 'object'}}
obj_count = collection.count_documents(filter_obj_type)
print(f"Documents with size_name as dict/object: {obj_count}")

# Show up to 5 sample documents to verify before deletion
if obj_count:
    print("Sample documents with dict size_name:")
    for doc in collection.find(filter_obj_type).limit(5):
        print(doc)

# Perform deletion
if obj_count:
    result = collection.delete_many(filter_obj_type)
    print(f"Deleted documents count: {result.deleted_count}")
else:
    print("No documents to delete.")
'''

# Close the connection
client.close()