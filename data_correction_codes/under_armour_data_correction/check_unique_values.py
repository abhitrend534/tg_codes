from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_underarmour_uk'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_price = collection.distinct('price')
unique_launch_price = collection.distinct('launch_price')
unique_gender = collection.distinct('gender')
unique_age_range = collection.distinct('age_range')

print(unique_gender)
print(unique_age_range)
print(unique_price)
print(unique_launch_price)

# Close the connection
client.close()