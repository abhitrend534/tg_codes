from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"   # <-- update with your actual URI
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_underarmour_uk'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_price = collection.distinct('price')
unique_launch_price = collection.distinct('launch_price')

for price in unique_price:
    if isinstance(price, str):
        try:
            new_price = float(price.strip())
            result = collection.update_many(
                {"price": price},
                {"$set": {"price": new_price}}
            )
            print(f"Updated price from '{price}' to {new_price}. Matched: {result.matched_count}, Modified: {result.modified_count}")
        except ValueError:
            print(f"Could not convert price '{price}' to float.")

for launch_price in unique_launch_price:
    if isinstance(launch_price, str):
        try:
            new_launch_price = float(launch_price.strip())
            result = collection.update_many(
                {"launch_price": launch_price},
                {"$set": {"launch_price": new_launch_price}}
            )
            print(f"Updated launch_price from '{launch_price}' to {new_launch_price}. Matched: {result.matched_count}, Modified: {result.modified_count}")
        except ValueError:
            print(f"Could not convert launch_price '{launch_price}' to float.")

# Close the connection
client.close()