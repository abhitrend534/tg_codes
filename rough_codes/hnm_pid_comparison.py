import json

india_pids = 'hnm_india_unique_pids.json'

with open(india_pids, 'r') as json_file:
    hnm_india_pids = json.load(json_file)

uk_pids = 'hnm_uk_brand_pids.json'

with open(uk_pids, 'r') as json_file:
    hnm_uk_pids = json.load(json_file)

print(hnm_uk_pids.keys())

temp = []
for i in ['ARKET', 'COS', 'Weekday', '& Other Stories', 'Monki']:
    for pid in hnm_uk_pids[i]:
        temp.append(pid)

c = set(hnm_india_pids).intersection(set(hnm_uk_pids['H&M']))

print(c)