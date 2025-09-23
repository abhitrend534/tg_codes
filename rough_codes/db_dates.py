import json

with open('db_collection_dates.json') as json_file:
    date_dict = json.load(json_file)

brands = []
for i in date_dict.keys():
    brandname = i.split('_')[2]
    brands.append(brandname)

brands = list(set(brands))

temp = {}
for i, j in date_dict.items():
    for brand in brands:
        if f'crawler_sink_{brand}' in i:
            dates = []
            for k in j:
                if '-04-' in k or '-05-' in k:
                    dates.append(k)
            if brand in temp.keys():
                temp[brand] += dates
            else:
                temp[brand] = dates

for i, j in temp.items():
    dates = list(set(j))
    dates.sort()
    temp[i] = dates
    print(i, dates)

with open('db_dates.json', 'w') as f:
    json.dump(temp, f, indent=4)