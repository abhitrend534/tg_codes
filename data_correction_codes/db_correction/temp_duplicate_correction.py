from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB connection details
connection1_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
connection2_string = "mongodb://localhost:27017/"
database_name = "tg_analytics"

# variables
melody_collection = 'crawler_sink_shein_india'
target_collection = 'temp'
collection1 = MongoClient(connection1_string)[database_name][melody_collection]
collection2 = MongoClient(connection2_string)[database_name][target_collection]

# Input date (ensure it's a datetime object)
target_date = '2025-07-23'
input_date = datetime.strptime(target_date, '%Y-%m-%d')
next_day = input_date + timedelta(days=1)

# Function to clean up the target collection
def clean_collection(collection, start_date, end_date):
    """
    Cleans up the target collection by removing documents where date_of_scraping
    is between start_date and end_date.
    """
    query = {
        "date_of_scraping": {
            "$gte": start_date,
            "$lt": end_date
        }
    }
    result = collection.delete_many(query)
    print(f"Removed {result.deleted_count} documents from {collection.name} for date range {start_date} to {end_date}.")

# Function to copy data from source_collection to target_collection
def copy_data(source_collection, target_collection, start_date, end_date, batch_size=1000):
    """
    Efficiently copies documents from source_collection to target_collection
    where date_of_scraping is between start_date and end_date, using batching.
    """
    query = {
        "date_of_scraping": {
            "$gte": start_date,
            "$lt": end_date
        }
    }

    cursor = source_collection.find(query, batch_size=batch_size)
    batch = []
    total_copied = 0

    for doc in cursor:
        doc.pop('_id', None)
        batch.append(doc)

        if len(batch) >= batch_size:
            target_collection.insert_many(batch)
            total_copied += len(batch)
            batch.clear()

    # Insert any remaining documents
    if batch:
        target_collection.insert_many(batch)
        total_copied += len(batch)

    print(f"Copied {total_copied} documents from {source_collection.name} to {target_collection.name}.")

# Function to remove duplicate SKUs from the target collection
def remove_duplicates_skus(collection):
    # Aggregate to find duplicate SKUs
    pipeline = [
        {"$group": {
            "_id": "$sku",
            "count": {"$sum": 1},
            "ids": {"$push": "$_id"}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = list(collection.aggregate(pipeline))
    
    # Remove duplicates
    for item in duplicates:
        ids = item['ids']
        # Keep the first document, remove the rest
        ids_to_remove = ids[1:]
        collection.delete_many({"_id": {"$in": ids_to_remove}})

    print(f"Removed {sum(len(item['ids']) - 1 for item in duplicates)} duplicate SKUs for {collection.name}.")

# Function to upload data to Melody
def upload_to_melody(source_collection, target_collection):
    BATCH_SIZE = 1000
    total_docs = 0  # <-- initialize before usage

    cursor = source_collection.find().batch_size(BATCH_SIZE)
    batch = []

    for doc in cursor:
        doc.pop('_id', None)  # Avoid duplicate _id insertion
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            target_collection.insert_many(batch, ordered=False)
            total_docs += len(batch)
            batch.clear()

    if batch:
        target_collection.insert_many(batch, ordered=False)
        total_docs += len(batch)

    print(f"Copied {total_docs} documents to {target_collection.name}")
    return {'status': 'success', 'copied_count': total_docs}

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
result = list(collection1.aggregate(pipeline))

# Print results
if result:
    total_count = result[0]["total_count"]
    unique_sku_count = result[0]["unique_sku_count"]
    if total_count == unique_sku_count:
        print("All SKUs are unique for the given date.")
    else:
        print(f"There are duplicate SKUs for the given date. Total documents: {total_count}, Unique SKUs: {unique_sku_count}")
        # clean up the target collection first
        print("Cleaning up the target collection...")
        clean_collection(collection2, input_date, next_day)
        # copy data from collection1 to collection2
        print("Copying data to the target collection now...")
        copy_data(collection1, collection2, input_date, next_day)
        print("Data copied successfully.")
        # remove duplicates from the target collection
        print("Removing duplicates from the target collection...")
        remove_duplicates_skus(collection2)
        print("Duplicates removed successfully.")
        # remove documents from collection1 for the given date
        print("Removing documents from the source collection for the given date...")
        start = datetime.strptime(target_date, '%Y-%m-%d')
        end = start.replace(hour=23, minute=59, second=59)
        collection1.delete_many({"date_of_scraping": {"$gte": start, "$lte": end}})
        print("Documents removed from the source collection successfully.")
        # upload to melody
        print("Uploading data to Melody now...")
        upload_to_melody(collection2, collection1)
        print("Data uploaded to Melody successfully.")
else:
    print("No documents found for the given date.")