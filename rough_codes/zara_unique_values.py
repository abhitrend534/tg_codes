import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

brands = {
    'zara': ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa']
}

key_fields = ['size_name']

for brand, geographies in brands.items():
    for key_field in key_fields:
        temp = []
        for geography in geographies:
            collection_name = f'crawler_sink_{brand}_{geography}'
            print(f'Searching {collection_name} now...')
            # Connect to MongoDB
            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]

            # Define the match criteria
            match_criteria = {
                "age_group": {"$not": {"$elemMatch": {"$eq": "adult"}}}  # Ensure "adult" is not in the array
            }

            # Retrieve unique values for the key field
            unique_values = collection.distinct(key_field, match_criteria)

            temp += list(unique_values)

        temp = list(set(temp))

        # Print the unique genders
        print(f"Unique {key_field} in {brand}: {temp}")

with open('zara_unique_values.json', 'w') as f:
    json.dump(temp, f, indent=4)

# Close the connection
client.close()