import json
import os

# Load a JSON file
json_file_path = 'data/json/web/0eb7bfe5-849f-41cc-917f-d54c3fe03ec3.json'
with open(json_file_path, 'r') as f:
    data = json.load(f)

print(f"Type of data: {type(data)}")
print(f"Number of items: {len(data)}")

# Find an item with non-empty page_visits and html_features
for i, item in enumerate(data):
    has_page_visits = 'page_visits' in item and len(item['page_visits']) > 0
    has_html_features = 'html_features' in item and len(item['html_features']) > 0

    if has_page_visits and has_html_features:
        print(f"\nFound item {i} with non-empty page_visits and html_features")
        print(f"visit_id: {item['visit_id']}")
        print(f"domain_name: {item['domain_name']}")

        # Check page_visits
        page_visits = item['page_visits']
        print(f"Number of page_visits: {len(page_visits)}")
        print(f"Keys in first page_visit: {list(page_visits[0].keys())}")

        # Check html_features
        html_features = item['html_features']
        print(f"Number of html_features: {len(html_features)}")
        print(f"Keys in first html_feature: {list(html_features[0].keys())}")

        # Check if response_body exists in page_visits
        if 'response_body' in page_visits[0]:
            print("response_body exists in page_visits")
            print(f"Length of response_body: {len(page_visits[0]['response_body'])}")
        else:
            print("response_body does not exist in page_visits")

        break
else:
    print("No items found with non-empty page_visits and html_features")
