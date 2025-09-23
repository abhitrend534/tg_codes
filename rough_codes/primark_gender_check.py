import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_primark_usa'
key_field = 'gender'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_genders = collection.distinct(key_field)

# Print the unique genders
print(f"Unique {key_field}:", unique_genders)

temp = {}

for gender in unique_genders:
    # Define the match criteria
    match_criteria = {
        "gender": gender
    }

    # Aggregate to find duplicate SKUs
    pipeline = [
        {"$match": match_criteria}
    ]

    uniques_pids = collection.distinct('product_id', match_criteria)

    temp[gender] = uniques_pids
    print(f'{gender}: {uniques_pids}')

with open('usa_unique_gender_pids.json', 'w') as f:
    json.dump(temp, f, indent=4)

# Close the connection
client.close()