import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

countries = ['Australia', 'Canada', 'India', 'Saudi', 'Spain', 'Turkey', 'UAE', 'UK', 'USA']

brand_dict = {}

for country in countries:
    brand_dict[country] = {}
    collection_name = f'crawler_sink_zara_{country.lower()}'
    collection = db[collection_name]

    # Aggregation pipeline to find product IDs with multiple unique lowercase titles
    pipeline = [
        {"$group": {
            "_id": "$product_id",
            "unique_titles": {"$addToSet": {"$toLower": "$title"}}
        }},
        {"$match": {"unique_titles.1": {"$exists": True}}}
    ]

    results = collection.aggregate(pipeline)

    for result in results:
        brand_dict[country][result["_id"]] = result["unique_titles"]

    print(f'{country} : {len(brand_dict[country])}')

with open('pid_with_different_title.json', 'w') as f:
    json.dump(brand_dict, f, indent=4)

# Close the connection
client.close()
