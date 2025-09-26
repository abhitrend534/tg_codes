from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

temp = {}

countries = ['india']
for country in countries:
    collection_name = f'crawler_sink_marknspencer_group_{country}'
    collection = db[collection_name]

    # Retrieve unique values for the key field
    unique_origins = collection.distinct('origin')

    print(f"Country: {country}, Unique Origins: {unique_origins}")


# Close the connection
client.close()