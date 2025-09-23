from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_lefties_spain'
key_field = 'size_name'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_genders = collection.distinct(key_field)

# Print the unique genders
print("Unique sizes:", unique_genders)

# Close the connection
client.close()
