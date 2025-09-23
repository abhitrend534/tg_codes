from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = 'tg_analytics'

geographies = ['uk', 'usa']

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

def get_age_group(age_range):
    new_born_ages = ['0m', '1m', '2m', '3m', '4m', '5m', '6m']
    baby_ages = ['7m', '8m', '9m', '10m', '11m', '12m', '13m', '14m', '15m', '16m', '17m', '18m', '19m', '20m', '21m', '22m', '23m', '24m']
    junior_ages = ['2y', '3y', '4y', '5y', '6y', '7y']
    senior_ages = ['8y', '9y', '10y', '11y', '12y']
    teen_ages = ['13y', '14y', '15y', '16y', '17y']
    adult_ages = ['18y']

    age_goup_list = ['new_born', 'baby', 'junior', 'senior', 'teen', 'adult']

    if len(age_range) == 1:
        if age_range[0] in new_born_ages:
            return ['new_born']
        elif age_range[0] in baby_ages:
            return ['baby']
        elif age_range[0] in junior_ages:
            return ['junior']
        elif age_range[0] in senior_ages:
            return ['senior']
        elif age_range[0] in teen_ages:
            return ['teen']
        elif age_range[0] in adult_ages:
            return ['adult']
    else:
        age_group = []
        start = age_range[0]
        end = age_range[-1]

        if start in new_born_ages:
            sindex = age_goup_list.index('new_born')
        elif start in baby_ages:
            sindex = age_goup_list.index('baby')
        elif start in junior_ages:
            sindex = age_goup_list.index('junior')
        elif start in senior_ages:
            sindex = age_goup_list.index('senior')
        elif start in teen_ages:
            sindex = age_goup_list.index('teen')
        elif start in adult_ages:
            sindex = age_goup_list.index('adult')
        
        if end in new_born_ages:
            eindex = age_goup_list.index('new_born')
        elif end in baby_ages:
            eindex = age_goup_list.index('baby')
        elif end in junior_ages:
            eindex = age_goup_list.index('junior')
        elif end in senior_ages:
            eindex = age_goup_list.index('senior')
        elif end in teen_ages:
            eindex = age_goup_list.index('teen')
        elif end in adult_ages:
            eindex = age_goup_list.index('adult')

        for i in range(sindex, eindex + 1):
            age_group.append(age_goup_list[i])

        if age_group == []:
            age_group = ['others']
            
        return age_group

def remap_age_range(age_range):
    if len(age_range) > 1:
        age_range = [age_range[0], age_range[-1]]

    if age_range[0] == '1y':
        if len(age_range) == 1:
            age_range = ['12m']
        else:
            end = age_range[-1]
            age_range = ['12m', end]

    if len(age_range) > 1 and age_range[-1] == '2y':
        end = '24m'
        age_range = ['12m', end]

    if len(age_range) > 1 and age_range[0] == '24m':
        end = str(int(int(age_range[-1][:-1])/12)) + 'y'
        age_range = ['2y', end]

    return age_range

def get_age_range(size):
    if 'Mths' in size:
        ranges = size.split(' ')[0].split('-')
        start = ranges[0]
        end = ranges[1]
        age_range = [start + 'm', end + 'm']
    if 'Yrs' in size:
        if '-' in size:
            ranges = size.split(' ')[0].split('-')
            start = ranges[0]
            end = ranges[1]
            age_range = [start + 'y', end + 'y']
        else:
            ranges = size.split(' ')[0].strip()
            age_range = [ranges + 'y']

    age_range = remap_age_range(age_range)
    return age_range

for geography in geographies:
    collection_name = f'crawler_sink_riverisland_{geography}'
    collection = db[collection_name]

    # Find documents and update product_id and brand_product_id
    for doc in collection.find({}, {"_id": 1, "age_group": 1, "size_name": 1}):
        age_group = doc.get("age_group")
        if age_group != ['adult']:
            size_name = doc.get("size_name")
            age_range = get_age_range(size_name)
            age_group = get_age_group(age_range)
            # Update the document
            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "age_range": age_range,
                        "age_group": age_group
                    }
                }
            )

    print(f'{collection_name} processing complete.')

# Close the connection
client.close()