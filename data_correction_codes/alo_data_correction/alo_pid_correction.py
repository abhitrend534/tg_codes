import json
from pymongo import MongoClient, UpdateOne

# Load JSON files
with open('alo_color_ids.json', 'r') as file:
    colors = json.load(file)

with open('alo_pid_remapping.json', 'r') as file:
    pid_mapping = json.load(file)

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_alo_usa'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Create reverse mapping for faster lookup - O(1) instead of O(n)
product_to_pid = {}
for pid, product_list in pid_mapping.items():
    for product_id in product_list:
        product_to_pid[product_id] = pid

def get_pid(product_id):
    clean_id = product_id.replace('alo', '')
    return product_to_pid.get(clean_id, clean_id)

def get_cid(color_name):
    return colors.get(color_name, '000')

# Prepare bulk operations with batch processing
operations = []
batch_size = 1000  # Process in batches to avoid memory issues

# Only fetch required fields to reduce memory usage and network transfer
projection = {'_id': 1, 'product_id': 1, 'color_name': 1, 'pid': 1, 'color_id': 1}

cursor = collection.find({}, projection)
processed_count = 0
total_updated = 0

for doc in cursor:
    processed_count += 1  # Count every record processed
    product_id = doc.get('product_id')
    color_name = doc.get('color_name')
    
    if not product_id or not color_name:
        continue
        
    new_pid = 'alo' + get_pid(product_id)
    color_id = f"{new_pid}%{get_cid(color_name)}"
    
    # Only update if values have changed
    current_pid = doc.get('product_id')
    current_color_id = doc.get('color_id')
    
    if current_pid != new_pid or current_color_id != color_id:
        operations.append(
            UpdateOne(
                {'_id': doc['_id']},
                {'$set': {'product_id': new_pid, 'color_id': color_id}}
            )
        )
    
    # Execute batch when reaching batch_size
    if len(operations) >= batch_size:
        if operations:
            result = collection.bulk_write(operations, ordered=False)
            total_updated += result.modified_count
            print(f"Processed {processed_count} records, Batch updated: {result.modified_count}, Total updated: {total_updated}")
            operations = []

# Execute remaining operations
if operations:
    result = collection.bulk_write(operations, ordered=False)
    processed_count += len(operations)
    print(f"Final batch processed. Total records processed: {processed_count}, Updated: {result.modified_count}")

print("Update operation completed successfully!")

# Close the connection
client.close()