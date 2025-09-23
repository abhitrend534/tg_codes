from pymongo import MongoClient, UpdateOne

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_skechers_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Prepare bulk operations
operations = []

try:
    # Only fetch documents that likely need updating to reduce work
    cursor = collection.find({
        '$or': [
            {'product_id': {'$regex': 'SKUS'}},
            {'color_id': {'$regex': 'SKUS'}},
            {'sku': {'$regex': 'SKUS'}},
        ]
    })

    for doc in cursor:
        update_fields = {}

        pid = doc.get('product_id')
        if isinstance(pid, str) and 'SKUS' in pid:
            update_fields['product_id'] = pid.replace('SKUS', 'skr')

        cid = doc.get('color_id')
        if isinstance(cid, str) and 'SKUS' in cid:
            update_fields['color_id'] = cid.replace('SKUS', 'skr')

        sku = doc.get('sku')
        if isinstance(sku, str) and 'SKUS' in sku:
            update_fields['sku'] = sku.replace('SKUS', 'skr')

        if update_fields:
            operations.append(
                UpdateOne({'_id': doc['_id']}, {'$set': update_fields})
            )

    # Execute bulk operations if there are any
    if operations:
        result = collection.bulk_write(operations)
        print(f"Bulk update - matched: {result.matched_count}, modified: {result.modified_count}")
    else:
        print("No documents to update.")

finally:
    # Close the connection
    client.close()