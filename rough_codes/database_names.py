import pymongo
import pandas as pd

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
read_database_name = "tg_analytics"

# Connect to MongoDB
client = pymongo.MongoClient(connection_string)
db = client[read_database_name]

# Get all collection names from the source database
collections = db.list_collection_names()

collection_names = [collection_name for collection_name in collections if collection_name.startswith('crawler_sink_')]

collection_names.sort()

brands = {}

# Print the collection names
for collection_name in collection_names:
    splits = collection_name.split('_')
    if len(splits) > 4:
        brand = splits[2] + splits[3]
    else:
        brand = splits[2]
    geography = splits[-1]

    if brand not in brands:
        brands[brand] = []
    brands[brand].append(geography)

print("Brands and their geographies:")
for brand, geographies in brands.items():
    print(f"{brand}: {', '.join(geographies)}")


list_of_brands = list(brands.keys())
pd.DataFrame(list_of_brands, columns=['brand']).to_csv('brands.csv', index=False)
