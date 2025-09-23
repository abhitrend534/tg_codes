import json

temp = {}

with open(f'unisex_dict.json') as json_file:
    unisex_dict = json.load(json_file)

print(unisex_dict)

for pid, dict in unisex_dict.items():
    if dict['gender'] not in temp.keys():
        temp[dict['gender']] = [pid]
    else:
        temp[dict['gender']].append(pid)

print(temp)

with open('temp_unique_gender_pids.json', 'w') as f:
    json.dump(temp, f, indent=4)