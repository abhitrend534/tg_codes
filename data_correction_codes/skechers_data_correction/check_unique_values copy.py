import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

temp = {}

countries = ['india', 'saudi', 'uae', 'uk', 'usa']
for country in countries:
    collection_name = f'crawler_sink_mango_{country}'
    collection = db[collection_name]

    # Retrieve unique values for the key field
    unique_sizes = collection.distinct('size_name')

    temp[country] = unique_sizes
    print(country, unique_sizes)

# Save the results to a JSON file
path = 'unique_sizes.json'
with open(path, 'w') as f:
    json.dump(temp, f, indent=4)

# Close the connection
client.close()