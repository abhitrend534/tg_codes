from pymongo import MongoClient
from datetime import datetime, timedelta
 
# Connect to MongoDB
client = MongoClient("mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin")
db = client["tg_analytics"]
collection = db["crawler_sink_lefties_uae"]

# Input date (ensure it's a datetime object)
input_date = datetime(2025, 7, 12)  # example date
next_day = input_date + timedelta(days=1)
 
# Aggregation pipeline
pipeline = [
    {
        "$match": {
            "date_of_scraping": {
                "$gte": input_date,
                "$lt": next_day
            }
        }
    },
    {
        "$group": {
            "_id": None,
            "total_count": { "$sum": 1 },
            "unique_skus": { "$addToSet": "$sku" }
        }
    },
    {
        "$project": {
            "_id": 0,
            "total_count": 1,
            "unique_sku_count": { "$size": "$unique_skus" }
        }
    }
]

# Run aggregation
result = list(collection.aggregate(pipeline))
 
# Print results
if result:
    print("Total documents:", input_date, result[0]["total_count"])
    print("Unique SKUs:", input_date, result[0]["unique_sku_count"])
else:
    print("No documents found for the given date.")