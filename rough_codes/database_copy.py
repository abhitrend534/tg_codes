import pymongo

# MongoDB connection details
connection1_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
connection2_string = "mongodb://localhost:27017/"
read_database_name = "tg_analytics"
target_database_name = "melody"

# Connect to MongoDB
client1 = pymongo.MongoClient(connection1_string)
client2 = pymongo.MongoClient(connection2_string)
source_db = client1[read_database_name]
target_db = client2[target_database_name]

# Get all collection names from the source database
collections = source_db.list_collection_names()

brands = ['riverisland', 'sacoor_brothers']

for brand in brands:
    for collection_name in collections:
        if collection_name.startswith(f'crawler_sink_{brand}'):
            source_collection = source_db[collection_name]
            target_collection = target_db[collection_name]
            
            # Copy all documents from source to target
            documents = list(source_collection.find())  # Fetch all documents
            if documents:
                target_collection.insert_many(documents)
                print(f"Copied {len(documents)} documents from {collection_name}")

    print(f"All collections of {brand} copied successfully!")