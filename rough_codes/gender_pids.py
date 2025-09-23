import json

countries = ['uk', 'usa']

temp = {}

with open(f'uk_unique_gender_pids.json') as json_file:
    uk_gender_dict = json.load(json_file)

with open(f'usa_unique_gender_pids.json') as json_file:
    usa_gender_dict = json.load(json_file)

male = list(set(uk_gender_dict['male'] + usa_gender_dict['male']))

female = list(set(uk_gender_dict['female'] + usa_gender_dict['female']))

unisex = list(set(uk_gender_dict['unisex'] + usa_gender_dict['unisex']))

temp['male'] = male
temp['female'] = female
temp['unisex'] = unisex

print(f'male: {len(male)}, female: {len(female)}, unisex: {len(unisex)}')

with open('unique_gender_pids.json', 'w') as f:
    json.dump(temp, f, indent=4)