import json

with open('unique_pids.json', 'r') as f:
    data = json.load(f)

for country, pids in data.items():
    print(f"{country}: {len(pids)} unique product IDs")
    temp = []
    for pid in pids:
        pid = pid.replace('_', '')
        temp.append(pid)
    data[country] = temp

with open('corrected_unique_pids.json', 'w') as f:
    json.dump(data, f, indent=4)

