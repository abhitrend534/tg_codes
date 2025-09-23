import json

with open('corrected_unique_pids.json', 'r') as f:
    data = json.load(f)

india_pids = data['india']
uk_pids = data['uk']
usa_pids = data['usa']

print(f"India PIDs: {len(india_pids)}")
print(f"UK PIDs: {len(uk_pids)}")
print(f"USA PIDs: {len(usa_pids)}")

# check common pids
uk_usa_common_pids = set(uk_pids) & set(usa_pids)
india_uk_common_pids = set(india_pids) & set(uk_pids)
india_usa_common_pids = set(india_pids) & set(usa_pids)
common_pids = set(india_pids) & set(usa_pids) & set(uk_pids)

print(f'uk_usa_common_pids: {len(uk_usa_common_pids)}')
print(f'india_uk_common_pids: {len(india_uk_common_pids)}')
print(f'india_usa_common_pids: {len(india_usa_common_pids)}')
print(f'common_pids: {len(common_pids)}')