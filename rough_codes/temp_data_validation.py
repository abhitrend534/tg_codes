from pymongo import MongoClient
from datetime import date, datetime

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"

# Create a MongoClient
client = MongoClient(connection_string)

# Get the list of database names
database_names = client.list_database_names()

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
today_str = '2024-11-29' #today.strftime('%Y-%m-%d')

# Important key field's value options
gender_options = ['male', 'female', 'infants', 'boys', 'girls', 'unisex']
age_group_options = ['new_born', 'baby', 'junior', 'senior', 'teen', 'adult']
availability_options = ['in_stock', 'out_of_stock', 'coming_soon', 'back_soon']

if 'tg_analytics' not in database_names:
    print("Database doesn't exist")
else:
    db = client['tg_analytics']
    collection_names = db.list_collection_names()
    for collection_name in collection_names:
        if f'crawler_sink_next' in collection_name:
            collection = db[collection_name]

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

            entries = list(collection.aggregate(pipeline))

            if len(entries) == 0:
                print(f'No data in {collection_name} for {today_str}')
            else:
                # Retrieve unique values from the collection
                uniques_genders = collection.distinct('gender', match_criteria)
                print(f'\nUnique genders from {collection_name} for {today_str} are {uniques_genders}')
                if all(item in gender_options for item in uniques_genders):
                    print(f'gender field for {collection_name} has correct entries.')
                else:
                    print(f'Error!!! gender field for {collection_name} has wrong entries.')

                uniques_age_groups = collection.distinct('age_group', match_criteria)
                print(f'\nUnique age_groups from {collection_name} for {today_str} are {uniques_age_groups}')
                if all(item in age_group_options for item in uniques_age_groups):
                    print(f'age_group field for {collection_name} has correct entries.')
                else:
                    print(f'Error!!! age_group field for {collection_name} has wrong entries.')

                uniques_availabilities = collection.distinct('availability', match_criteria)
                print(f'\nUnique availabilities from {collection_name} for {today_str} are {uniques_availabilities}')
                if all(item in availability_options for item in uniques_availabilities):
                    print(f'availability field for {collection_name} has correct entries.')
                else:
                    print(f'Error!!! availability field for {collection_name} has wrong entries.')