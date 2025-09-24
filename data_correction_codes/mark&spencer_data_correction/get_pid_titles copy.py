import json
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'

# Connect to MongoDB
client = None
try:
    client = MongoClient(connection_string)
    db = client[database_name]
    
    temp = {}
    
    countries = ['india', 'uk', 'usa']
    for country in countries:
        temp[country] = {}
        collection_name = f'crawler_sink_marknspencer_{country}'
        collection = db[collection_name]
    
        # Retrieve unique values for the key field
        unique_pids = collection.distinct('product_id')
    
        # retrieve last title for each pid
        for pid in unique_pids:
            titles = collection.distinct('title', {'product_id': pid})
            temp[country][pid] = titles
    
    # Save the results to a JSON file
    path = 'unique_pid_title_list.json'
    with open(path, 'w') as f:
        json.dump(temp, f, indent=4)
    
    print(f"Successfully saved titles for {sum(len(titles) for titles in temp.values())} products to {path}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    if client is not None:
        client.close()