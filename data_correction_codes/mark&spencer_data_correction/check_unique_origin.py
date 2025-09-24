from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

temp = {}

countries = ['india']
for country in countries:
    collection_name = f'crawler_sink_marknspencer_{country}'
    collection = db[collection_name]

    # Retrieve unique values for the key field
    unique_origins = collection.distinct('origin')

    print(f"Country: {country}, Unique Origins: {unique_origins}")


# Close the connection
client.close()