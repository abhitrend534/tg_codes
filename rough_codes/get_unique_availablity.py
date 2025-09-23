import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
key_field = 'title'

target_prefix = 'crawler_sink_gymshark'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

temp = {}
unique_title_values = set()

for collection_name in db.list_collection_names():
    if target_prefix in collection_name:
        collection = db[collection_name]

        # Retrieve unique values for the key field
        unique_titles = collection.distinct(key_field)
        print(f"Unique titles for {collection_name}:", unique_titles)

        # Store the unique titles in the temp dictionary
        temp[collection_name] = unique_titles
        unique_title_values.update(unique_titles)

temp['unique_title_values'] = list(unique_title_values)

# Print the unique title values
print("Unique title values across all collections:", temp['unique_title_values'])

# Save the unique values to a JSON file
with open('unique_title_values.json', 'w') as f:
    json.dump(temp, f, indent=4)

# Close the connection
client.close()
