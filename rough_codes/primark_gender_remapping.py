import json
from pymongo import MongoClient

with open(f'temp_unique_gender_pids.json') as json_file:
    gender_dict = json.load(json_file)

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = 'tg_analytics'

geographies = ['usa', 'uk']

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

for geography in geographies:
    print(f'starting for {geography}...')
    collection_name = f'crawler_sink_primark_{geography}'
    collection = db[collection_name]

    for gender, pids in gender_dict.items():
        print(f'starting for {geography} {gender}...')
        if gender != 'unisex':
            for pid in pids:
                # Define the match criteria
                match_criteria = {
                    "product_id": pid
                }

                # Update the document
                collection.update_many(
                    match_criteria,
                    {
                        "$set": {
                            'gender': gender
                        }
                    }
                )
                print(f'{pid} updated.')
        print(f'{geography} {gender} finished.')

# Close the connection
client.close()