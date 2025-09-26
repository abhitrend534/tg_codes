import json

# Load JSON files
with open('alo_color_ids.json', 'r') as file:
    colors = json.load(file)

# Load JSON files
with open('unique_alo_colors.json', 'r') as file:
    unique_colors = json.load(file)

# Find missing colors
missing_colors = [color for color in unique_colors if color not in colors.keys()]

print(len(missing_colors))

# Save missing colors to a JSON file
for color in missing_colors:
    colors[color] = f"{len(colors) + 1:03d}"

with open('alo_color_ids.json', 'w') as file:
    json.dump(colors, file, indent=4)