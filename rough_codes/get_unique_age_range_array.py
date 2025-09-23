import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

brands = {
    'zara': ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa'],
    'next': ['india', 'saudi', 'uae', 'uk'],
    'lefties': ['saudi', 'spain', 'turkey', 'uae'],
    'riverisland': ['uk', 'usa']
}

key_fields = ['age_range']

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

            # Retrieve documents and filter for unique arrays
            pipeline = [
                {"$group": {"_id": f"${key_field}"}},  # Group by the `age_range` field
                {"$project": {"_id": 0, "age_range": "$_id"}}  # Format output
            ]
            unique_age_ranges = list(collection.aggregate(pipeline))

            # Extract and print unique age ranges
            unique_age_ranges = [entry["age_range"] for entry in unique_age_ranges]

            temp += list(unique_age_ranges)

        temp = [list(t) for t in {tuple(entry) for entry in temp}]
        brand_dict[brand][key_field] = temp

        # Print the unique genders
        print(f"Unique {key_field} in {brand}: {temp}")

with open('unique_age_ranges.json', 'w') as f:
    json.dump(brand_dict, f, indent=4)

# Close the connection
client.close()