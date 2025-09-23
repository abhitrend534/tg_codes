import json
from datetime import datetime
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'

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

brands = {
    'zara': ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa'],
    'pull&bear': ['australia', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa'],
    'stradivarius': ['canada', 'saudi', 'spain', 'uae', 'uk', 'usa'],
    'bershka': ['canada', 'saudi', 'spain', 'uae', 'uk', 'usa'],
    'mango': ['india', 'saudi', 'uae', 'uk', 'usa'],
    'lefties': ['saudi', 'spain', 'turkey', 'uae'],
    'next': ['india', 'saudi', 'uae', 'uk'],
    'sacoor_brothers': ['saudi', 'uae'],
    'riverisland': ['uk', 'usa'],
    'primark': ['uk', 'usa'],
    'gant': ['uae']
}

brand_dict = {}

for brand, geographies in brands.items():
    brand_dict[brand] = {}
    for geography in geographies:
        collection_name = f'crawler_sink_{brand}_{geography}'
        brand_dict[brand][collection_name] = {}
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]

        # Retrieve unique values for the key field
        unique_dates = collection.distinct('date_of_scraping')
        unique_dates.sort()

        for target_date in unique_dates:
            date_str = target_date.strftime('%Y-%m-%d')

            # Convert the date to the MongoDB format
            db_date = parse_launch_date(date_str)

            # Define the match criteria
            match_criteria = {
                "date_of_scraping": db_date
            }

            # Aggregate to find duplicate SKUs
            pipeline = [
                {"$match": match_criteria}
            ]

            entries = list(collection.aggregate(pipeline))
            uniques_skus = collection.distinct('sku', match_criteria)

            total = len(entries)
            skus = len(uniques_skus)
            difference = total - skus

            temp = {'total_entries': total, 'unique_entries': skus, 'difference': difference}
            brand_dict[brand][collection_name][date_str] = temp

            print(brand, geography, date_str, temp)

    with open(f'{brand}_sku_report.json', 'w') as f:
        json.dump(brand_dict, f, indent=4)

# Close the connection
client.close()