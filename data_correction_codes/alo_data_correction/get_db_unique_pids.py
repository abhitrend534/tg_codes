import json
from pymongo import MongoClient

# Load JSON files
with open('alo_color_ids.json', 'r') as file:
    colors = json.load(file)

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

temp = set()

countries = ['canada', 'uk', 'usa']
for country in countries:
    collection_name = f'crawler_sink_alo_{country}'
    collection = db[collection_name]

    # Retrieve unique values for the key field
    unique_color_ids = collection.distinct('color_id')

    for color_id in unique_color_ids:
        if color_id:
            temp.add(color_id.split('%')[1])  # Extract part after '%'

#save to json
with open('unique_alo_color_ids.json', 'w') as file:
    json.dump(list(temp), file, indent=4)

# Close the connection
client.close()