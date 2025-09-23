import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

collections = db.list_collection_names()

date_dict = {}

for collection_name in collections:
    if 'crawler_sink' in collection_name:
        print(f'Fetching {collection_name} dates...')
        collection = db[collection_name]

        # Retrieve unique values for the key field
        unique_dates = collection.distinct('date_of_scraping')
        unique_dates.sort()

        # Convert datetime to string format YYYY-MM-DD
        formatted_dates = [d.strftime('%Y-%m-%d') for d in unique_dates]

        print(f'{collection_name} : {formatted_dates}')
        date_dict[collection_name] = formatted_dates

brands = []
for i in date_dict.keys():
    brandname = i.split('_')[2]
    brands.append(brandname)

brands = list(set(brands))

temp = {}
for i, j in date_dict.items():
    for brand in brands:
        if f'crawler_sink_{brand}' in i:
            dates = []
            for k in j:
                if '2025-06-' in k:
                    dates.append(k)
            if brand in temp.keys():
                temp[brand] += dates
            else:
                temp[brand] = dates

for i, j in temp.items():
    dates = list(set(j))
    dates.sort()
    temp[i] = dates
    print(i, dates)

with open('db_dates.json', 'w') as f:
    json.dump(temp, f, indent=4)

temp_dict = {}
dlist = list(temp.keys())
dlist.sort()

for i in dlist:
    temp_dict[i] = temp[i]

with open('sorted_db_dates.json', 'w') as f:
    json.dump(temp_dict, f, indent=4)

# Close the connection
client.close()