import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_primark_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Load JSON file containing product IDs
with open('unisex.json') as json_file:
    unisex_list = json.load(json_file)

unisex_dict = {}

for pid in unisex_list:
    # Define the match criteria
    match_criteria = {"product_id": pid}

    # Fetch the entry
    entry = collection.find_one(match_criteria)

    temp = []
    if entry:
        images = entry.get('images', 'No images found')
        for image in images:
            temp.append(image['url'])
        print(f'{pid}: {temp}')
    else:
        print(f'Product ID {pid} not found in the database.')

    unisex_dict[pid] = {'gender': '', 'images': temp}

collection_name = 'crawler_sink_primark_uk'
collection = db[collection_name]

for pid, images in unisex_dict.items():
    if images['images'] == []:
        # Define the match criteria
        match_criteria = {"product_id": pid}

        # Fetch the entry
        entry = collection.find_one(match_criteria)

        temp = []
        if entry:
            images = entry.get('images', 'No images found')
            for image in images:
                temp.append(image['url'])
            print(f'{pid}: {temp}')
        else:
            print(f'Product ID {pid} not found in the database.')

        unisex_dict[pid] = {'gender': '', 'images': temp}

with open('unisex_dict.json', 'w') as f:
    json.dump(unisex_dict, f, indent=4)

# Close the connection
client.close()
