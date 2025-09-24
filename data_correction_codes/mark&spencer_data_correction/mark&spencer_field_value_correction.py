from pymongo import MongoClient, UpdateMany

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'footwear_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

countries = ['india', 'uk', 'usa']
for country in countries:
    collection_name = f'crawler_sink_marknspencer_{country}'
    collection = db[collection_name]

    fields_to_check = [
        'sole_material',
        'upper_material',
        'closure_type',
        'toe_type',
        'heel_type',
        'weight',
        'heel_to_toe_drop',
        'occasion'
    ]

    # Prepare bulk operations
    operations = []

    # Find documents that have empty strings or 'null' values in the specified fields
    for field in fields_to_check:
        # Create filter to find documents with empty string or 'null' string values
        filter_query = {field: {'$in': ['', 'null']}}
        
        # Create update operation to set the field to None
        update_query = {'$set': {field: None}}
        
        # Add to bulk operations
        operations.append(UpdateMany(filter_query, update_query))

    if operations:
        result = collection.bulk_write(operations)
        print(f"Bulk update result: {result.bulk_api_result}")

# Close the connection
client.close()