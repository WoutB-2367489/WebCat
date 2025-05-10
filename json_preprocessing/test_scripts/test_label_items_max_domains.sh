#!/bin/bash

# Create output directory if it doesn't exist
mkdir -p data/labels

# Run the labeling script with max-domains parameter
echo "Starting the labeling process with max-domains=2..."
python labeller/label_items.py data/parquet/html-features.parquet data/json/web data/labels/max_domains_labels.parquet --max-domains 2

# Check if labeling was successful
if [ $? -ne 0 ]; then
  echo "Error: Labeling process failed"
  exit 1
fi

# Display the labels if they exist
if [ -f "data/labels/max_domains_labels.parquet" ]; then
  echo "Labeling completed successfully!"
  echo "Labels saved to data/labels/max_domains_labels.parquet"
  
  # Use Python to display the first few rows of the labels file
  python -c "import pandas as pd; print(pd.read_parquet('data/labels/max_domains_labels.parquet').head())"
  
  # Count the number of distinct domains labeled
  python -c "
import pandas as pd
import json
import glob
import os

# Load the labels
labels_df = pd.read_parquet('data/labels/max_domains_labels.parquet')
labeled_visit_ids = labels_df[labels_df['label'] != -1]['visit_id'].tolist()

# Find the corresponding URLs for labeled visit_ids
json_folder = 'data/json/web'
domain_labels = {}

for visit_id in labeled_visit_ids:
    json_files = glob.glob(os.path.join(json_folder, '*.json'))
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if item.get('visit_id') == visit_id:
                        if 'page_visits' in item and item['page_visits']:
                            for page_visit in item['page_visits']:
                                if page_visit.get('status_code') == 200 and 'url' in page_visit:
                                    url = page_visit['url']
                                    domain_labels[url] = True
                                    break
        except Exception as e:
            print(f'Error reading {json_file}: {e}')

print(f'Number of distinct domains labeled: {len(domain_labels)}')
print(f'Domains: {list(domain_labels.keys())}')
"
else
  echo "Error: Labels file not found"
  exit 1
fi

echo "Test completed successfully!"