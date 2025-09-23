import json
from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_kith_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

with open('unique_prices.json', 'r') as file:
    unique_prices = json.load(file)

# Prepare bulk update operations
operations = []

for pid, price_dict in unique_prices.items():
    if 'price' in price_dict and 'launch_price' in price_dict:
        operations.append(
            UpdateMany(
                {'product_id': pid},
                {'$set': {
                    'price': price_dict['price'],
                    'launch_price': price_dict['launch_price']
                }}
            )
        )

# Execute bulk update operations if there are any
if operations:   
    result = collection.bulk_write(operations)
    print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# Close the connection
client.close()