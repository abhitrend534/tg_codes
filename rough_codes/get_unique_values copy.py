import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

brands = {
    'gant': ['uae']
}

key_fields = ['date_of_scraping']

brand_dict = {}

for brand, geographies in brands.items():
    for key_field in key_fields:
        temp = []
        dates = []
        for geography in geographies:
            collection_name = f'crawler_sink_{brand}_{geography}'
            # Connect to MongoDB
            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]

            # Retrieve unique values for the key field
            unique_values = collection.distinct(key_field)

            temp += list(unique_values)

        for i in list(set(temp)):
            dates.append(i.strftime('%Y-%m-%d'))
        dates.sort()
        brand_dict[brand] = dates

        # Print the unique genders
        print(f"Unique {key_field} in {brand}: {dates}")

with open('unique_dates_values.json', 'w') as f:
    json.dump(brand_dict, f, indent=4)

# Close the connection
client.close()