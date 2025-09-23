from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

collection_name = 'crawler_sink_mango_usa'
collection = db[collection_name]

# Retrieve unique values for the key field
unique_dates = collection.distinct('date_of_scraping')

unique_dates.sort()
print(len(unique_dates), unique_dates)

# Close the connection
client.close()