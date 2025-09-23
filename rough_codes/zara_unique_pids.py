from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

countries = ['India', 'Saudi', 'UAE', 'UK']

for country in countries:
    collection_name = f'crawler_sink_h&m_{country.lower()}'
    collection = db[collection_name]

    # Retrieve unique values for the key field
    unique_pids = collection.distinct('product_id')

    for pid in unique_pids:
        # Define the match criteria
        match_criteria = {
            "product_id": pid
        }

        # Aggregate to find duplicate SKUs
        pipeline = [
            {"$match": match_criteria}
        ]

        unique_titles = collection.distinct('title', pipeline)

        if len(unique_titles) > 1:
            print(f'{pid} : {unique_titles}')

# Close the connection
client.close()