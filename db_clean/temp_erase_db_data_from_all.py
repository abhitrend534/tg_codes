from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_names = ['footwear_analytics', 'tg_analytics']

brand = '_nike'

# Connect to MongoDB
client = MongoClient(connection_string)
for database_name in database_names:
    print(f'Processing database: {database_name}')
    db = client[database_name]

    collection_names = db.list_collection_names()

    for collection_name in collection_names:
        if 'v3' in collection_name and '_images_download' not in collection_name and brand in collection_name and collection_name not in [f'v3_products{brand}', f'v3_colors{brand}']:
            if ('v3_products' in collection_name or 'v3_colors' in collection_name) and not any(country in collection_name for country in ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa', 'korea']):
                continue

            print(f'Cleaning the {collection_name} collection...')
            collection = db[collection_name]
            collection.delete_many({})

# Close the connection
client.close()