from pymongo import MongoClient
from datetime import datetime

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

collection_name = f'v3_products_bershka'
collection = db[collection_name]

# Retrieve unique values for the key field
unique_pids1 = collection.distinct('product_id')

print(len(unique_pids1))

# Close the connection
client.close()

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

def parse_launch_date(date_string):
    format_string_with_ms = '%Y-%m-%dT%H:%M:%S.%fZ'
    format_string_without_ms = '%Y-%m-%dT%H:%M:%SZ'
    format_string_date_only = '%Y-%m-%d'
    format_string_with_ms_no_tz = '%Y-%m-%d %H:%M:%S.%f'
    
    try:
        return datetime.strptime(date_string, format_string_with_ms)
    except ValueError:
        try:  
            return datetime.strptime(date_string, format_string_without_ms)
        except ValueError:
            try:
                return datetime.strptime(date_string, format_string_date_only)
            except ValueError:
                return datetime.strptime(date_string, format_string_with_ms_no_tz)

countries = ['canada', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'uae']

today_str = '2025-03-10'

# Convert the date to the MongoDB format
db_date = parse_launch_date(today_str)

pids = []

for country in countries:
    collection_name = f'crawler_sink_bershka_{country}'
    collection = db[collection_name]

    # Define the match criteria
    match_criteria = {
        "date_of_scraping": db_date
    }

    # Retrieve unique values for the key field
    unique_pids = collection.distinct('product_id', match_criteria)

    pids += unique_pids

unique_pids2 = list(set(pids))

print(set(unique_pids2) - set(unique_pids1))