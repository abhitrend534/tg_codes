import json

with open('db_dates.json') as json_file:
    date_dict = json.load(json_file)

temp = {}
dlist = list(date_dict.keys())
dlist.sort()

for i in dlist:
    temp[i] = date_dict[i]

with open('sorted_db_dates.json', 'w') as f:
    json.dump(temp, f, indent=4)

print('file sorted.')