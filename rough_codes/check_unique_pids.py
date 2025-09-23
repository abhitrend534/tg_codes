from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_paige_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_pids = collection.distinct('color_id')

print(len(unique_pids))
print(unique_pids)

# Close the connection
client.close()