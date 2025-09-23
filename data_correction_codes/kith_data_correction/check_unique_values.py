import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_lewkin_south_korea'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

temp1 = {}
temp2 = {}

prices = collection.distinct('price')
launch_prices = collection.distinct('launch_price')
print(f"Distinct Prices: {prices}, \n\nDistinct Launch Prices: {launch_prices}")

# Use aggregation to get unique prices and launch prices per product_id
pipeline = [
    {
        "$group": {
            "_id": "$product_id",
            "unique_prices": {"$addToSet": "$price"},
            "unique_launch_prices": {"$addToSet": "$launch_price"}
        }
    }
]
results = collection.aggregate(pipeline)

for doc in results:
    pid = doc['_id']
    unique_prices = sorted([p for p in doc.get('unique_prices', []) if p is not None])
    unique_launch_prices = sorted([lp for lp in doc.get('unique_launch_prices', []) if lp is not None])

    temp1[pid] = {
        'unique_prices': unique_prices,
        'unique_launch_prices': unique_launch_prices
    }
    temp2[pid] = {
        'price': unique_prices[0] if unique_prices else None,
        'launch_price': unique_launch_prices[0] if unique_launch_prices else None
    }

# Save the results to a JSON file
path1 = 'unique_prices_list.json'
with open(path1, 'w') as f:
    json.dump(temp1, f, indent=4)

path2 = 'unique_prices.json'
with open(path2, 'w') as f:
    json.dump(temp2, f, indent=4)

# Close the connection
client.close()