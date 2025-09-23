import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'melody'
collection_name = 'crawler_sink_h&m_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_image_urls = collection.distinct('images.url')

print(len(unique_image_urls))

# Close the connection
client.close()

# Save the unique image URLs to a json file
with open('unique_hnm_image_urls.json', 'w') as file:
    json.dump(unique_image_urls, file, indent=4)