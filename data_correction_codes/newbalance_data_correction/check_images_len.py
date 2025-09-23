from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = f'crawler_sink_newbalance_uae'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

distinct_sizes = set()

for entry in collection.find({}, {'images': 1, 'url': 1, 'date_of_scraping' : 1}):
    images = entry.get('images', [])
    if isinstance(images, list):
        if len(images) > 12:
            print(entry['url'], len(images), entry.get('date_of_scraping'))
        distinct_sizes.add(len(images))

print("Distinct lengths of images arrays:", distinct_sizes)

# Close the connection
client.close()