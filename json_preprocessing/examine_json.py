#!/usr/bin/env python3
import json
import sys

# Path to a sample JSON file
json_file_path = "/home/woutb/projects/WebCat/test_json_folder/60e83ae3-869b-45ec-a09e-32c0dc38e05c.json"

# Load the JSON data
with open(json_file_path, 'r') as f:
    data = json.load(f)

# Check if it's a list and get the first item
if isinstance(data, list) and len(data) > 0:
    item = data[0]
else:
    item = data

# Print the keys at the top level
print("Top-level keys:", list(item.keys()))

# Check if 'html_features' exists
if 'html_features' in item:
    html_features = item['html_features']
    print("\nType of 'html_features':", type(html_features))

    # Handle the case where html_features is a list
    if isinstance(html_features, list):
        print(f"'html_features' is a list with {len(html_features)} items")
        if len(html_features) > 0:
            print("\nExamining the first item in the 'html_features' list:")
            first_item = html_features[0]
            if isinstance(first_item, dict):
                print("Keys in the first item:", list(first_item.keys()))
                # Print a sample of values for each key
                print("\nSample values from the first item:")
                for key, value in first_item.items():
                    value_str = str(value)
                    print(f"  {key}: {value_str[:100]}{'...' if len(value_str) > 100 else ''}")
            else:
                print("The first item is not a dictionary, it's a:", type(first_item))
                print("Value:", str(first_item)[:100])
    # Handle the case where html_features is a dictionary
    elif isinstance(html_features, dict):
        print("\nKeys under 'html_features':", list(html_features.keys()))
        # Print a sample of values for each key
        print("\nSample values under 'html_features':")
        for key, value in html_features.items():
            value_str = str(value)
            print(f"  {key}: {value_str[:100]}{'...' if len(value_str) > 100 else ''}")
    else:
        print("'html_features' is neither a list nor a dictionary, it's a:", type(html_features))
        print("Value:", str(html_features)[:100])
else:
    print("\n'html_features' key not found in the JSON data")
