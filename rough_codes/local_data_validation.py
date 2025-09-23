import json
from pymongo import MongoClient
from datetime import date, datetime

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'

brands = {
    'bershka': ['canada', 'saudi', 'spain', 'uae', 'uk', 'usa'],
    'lefties': ['saudi', 'spain', 'turkey', 'uae'],
    'next': ['india', 'saudi', 'uae', 'uk'],
    'primark': ['uk', 'usa'],
    'pull&bear': ['australia', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa'],
    'riverisland': ['uk', 'usa'],
    'sacoor_brothers': ['saudi', 'uae'],
    'stradivarius': ['canada', 'saudi', 'spain', 'uae', 'uk', 'usa'],
    'zara': ['australia', 'canada', 'india', 'saudi', 'spain', 'turkey', 'uae', 'uk', 'usa']
}

key_fields = ['gender', 'age_group', 'age_range']

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
            
today = date.today()
#today_str = '2024-11-29' #today.strftime('%Y-%m-%d')

dates = ['2025-01-09', '2025-01-10']

for brand, geographies in brands.items():
    print(f'Checking {brand} now...')
    brand_dict = {}
    for key_field in key_fields:
        temp = []
        for geography in geographies:
            collection_name = f'crawler_sink_{brand}_{geography}'
            # Connect to MongoDB
            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]

            for today_str in dates:
                # Convert the date to the MongoDB format
                db_date = parse_launch_date(today_str)

                # Define the match criteria
                match_criteria = {
                    "date_of_scraping": db_date
                }

                # Aggregate to find duplicate SKUs
                pipeline = [
                    {"$match": match_criteria}
                ]

                # Retrieve unique values for the key field
                unique_values = collection.distinct(key_field, match_criteria)

                temp += list(unique_values)

        temp = list(set(temp))
        brand_dict[key_field] = temp

        # Print the unique genders
        print(f"Unique {key_field} in {brand}: {temp}")

    with open(f'{brand}_unique_values.json', 'w') as f:
        json.dump(brand_dict, f, indent=4)

# Close the connection
client.close()