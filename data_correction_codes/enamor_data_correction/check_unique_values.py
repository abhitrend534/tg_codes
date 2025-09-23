from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_enamor_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_prices = collection.distinct('price')
unique_launch_prices = collection.distinct('launch_price')

print(unique_prices)
print(unique_launch_prices)

# Close the connection
client.close()