from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_gant_uae'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Retrieve unique values for the key field
unique_cids = collection.distinct('color_id')

for cid in unique_cids:
    if 'unknown' in cid:
        print(cid)


# Close the connection
client.close()