import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

brands = {
    'bershka': ['canada', 'saudi', 'spain', 'uae', 'uk', 'usa'],
    'lefties': ['saudi', 'spain', 'turkey', 'uae'],
    'next': ['india', 'saudi', 'uae', 'uk'],
    'primark': ['uk', 'usa'],
    'pull&bear': ['australia', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa'],
    'riverisland': ['uk', 'usa'],
    'sacoor_brothers': ['saudi', 'uae'],
    'stradivarius': ['canada', 'saudi', 'spain', 'uae', 'uk', 'usa'],
    'zara': ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa']
}

key_fields = ['gender', 'age_group', 'age_range']

brand_dict = {}

for brand, geographies in brands.items():
    brand_dict[brand] = {}
    for key_field in key_fields:
        temp = []
        for geography in geographies:
            collection_name = f'crawler_sink_{brand}_{geography}'
            # Connect to MongoDB
            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]

            # Retrieve unique values for the key field
            unique_values = collection.distinct(key_field)

            temp += list(unique_values)

        temp = list(set(temp))
        brand_dict[brand][key_field] = temp

        # Print the unique genders
        print(f"Unique {key_field} in {brand}: {temp}")

with open('unique_values.json', 'w') as f:
    json.dump(brand_dict, f, indent=4)

# Close the connection
client.close()