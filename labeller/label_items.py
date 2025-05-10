#!/usr/bin/env python3
import os
import sys
import json
import glob
import argparse
import webbrowser
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException

def find_json_file_for_visit_id(visit_id, json_folder):
    """Find the JSON file that contains the given visit_id."""
    json_files = glob.glob(os.path.join(json_folder, "*.json"))

    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if item.get('visit_id') == visit_id:
                        return json_file, item
        except Exception as e:
            print(f"Error reading {json_file}: {e}")

    return None, None

def get_url(item):
    """Extract URL from the page_visits field."""
    if 'page_visits' not in item or not item['page_visits']:
        return None

    # Find the first page visit with a 200 status code and a URL
    for page_visit in item['page_visits']:
        if page_visit.get('status_code') == 200 and 'url' in page_visit:
            return page_visit['url']

    return None

def label_items(html_features_path, json_folder, output_path, max_domains=None):
    """Label each item in the html-features.parquet file.

    Args:
        html_features_path: Path to the html-features.parquet file
        json_folder: Path to the folder containing JSON files
        output_path: Path to save the labels.parquet file
        max_domains: Maximum number of distinct domains to label (optional)
    """
    print(f"Loading {html_features_path}...")

    # Load the html-features.parquet file
    df = pd.read_parquet(html_features_path)
    print(f"Loaded {len(df)} rows from {html_features_path}")

    # Create a dictionary to store labels
    labels = {}

    # Create a dictionary to store domain/URL to label mapping
    domain_labels = {}

    # Initialize the browser
    browser = None
    try:
        print("Initializing browser...")
        options = Options()
        options.add_argument("--start-maximized")  # Start with window maximized
        browser = webdriver.Chrome(options=options)
        print("Browser initialized successfully")
    except Exception as e:
        print(f"Error initializing browser: {e}")
        print("Falling back to default webbrowser module")

    # Check if output file exists and load previous labels
    if os.path.exists(output_path):
        try:
            previous_labels = pd.read_parquet(output_path)
            print(f"Loaded {len(previous_labels)} previous labels from {output_path}")

            # Process previous labels to create visit_id to label mapping
            for _, row in previous_labels.iterrows():
                if row['label'] != -1:  # Only consider labeled items
                    labels[row['visit_id']] = row['label']

            # Find the corresponding URLs for previously labeled visit_ids
            for visit_id, label in labels.items():
                json_file, item = find_json_file_for_visit_id(visit_id, json_folder)
                if json_file and item:
                    url = get_url(item)
                    if url:
                        domain_labels[url] = label

            print(f"Found {len(domain_labels)} previously labeled domains")
        except Exception as e:
            print(f"Error loading previous labels: {e}")
            print("Starting with empty labels")

    # Process each row
    stop_labeling = False
    for i, row in df.iterrows():
        if stop_labeling:
            break

        visit_id = row['visit_id']
        domain_name = row['domain_name']

        print(f"\nProcessing item {i+1}/{len(df)}")
        print(f"visit_id: {visit_id}")
        print(f"domain_name: {domain_name}")

        # Find the JSON file for this visit_id
        json_file, item = find_json_file_for_visit_id(visit_id, json_folder)

        if json_file and item:
            print(f"Found JSON file: {json_file}")

            # Get the URL
            url = get_url(item)

            if url:
                print(f"Found URL: {url}")

                # Check if this URL has already been labeled
                if url in domain_labels:
                    # Automatically apply the previous label
                    previous_label = domain_labels[url]
                    labels[visit_id] = previous_label
                    print(f"URL already labeled as {previous_label}. Automatically applying this label.")
                else:
                    # Open the URL in the browser
                    print(f"Opening URL in browser...")
                    if browser:
                        try:
                            browser.get(url)
                            # Give the page some time to load
                            time.sleep(1)
                        except WebDriverException as e:
                            print(f"Error opening URL in Selenium: {e}")
                            print("Falling back to default webbrowser module")
                            webbrowser.open(url)
                    else:
                        webbrowser.open(url)

                    # Ask for label
                    while True:
                        label = input("Label this item (0 or 1, or 's' to skip): ")
                        if label in ['0', '1']:
                            label_value = int(label)
                            labels[visit_id] = label_value
                            # Store the label for this URL for future reference
                            domain_labels[url] = label_value

                            # Check if we've reached the maximum number of domains
                            if max_domains is not None and len(domain_labels) >= max_domains:
                                print(f"\nReached the maximum number of distinct domains ({max_domains}). Stopping.")
                                stop_labeling = True
                                break
                            break
                        elif label.lower() == 's':
                            print("Skipping this item")
                            break
                        else:
                            print("Invalid input. Please enter 0, 1, or 's'.")
            else:
                print(f"No URL found for visit_id {visit_id}")
        else:
            print(f"No JSON file found for visit_id {visit_id}")

    # Create a DataFrame with the labels
    labels_df = pd.DataFrame({
        'visit_id': list(labels.keys()),
        'label': list(labels.values())
    })

    # Ensure the labels DataFrame has the same order as the html-features DataFrame
    if len(labels_df) > 0:
        # Create a mapping from visit_id to label
        label_map = dict(zip(labels_df['visit_id'], labels_df['label']))

        # Create a new DataFrame with all visit_ids from the html-features DataFrame
        all_labels_df = pd.DataFrame({'visit_id': df['visit_id']})

        # Add the label column, using the label_map
        all_labels_df['label'] = all_labels_df['visit_id'].map(label_map)

        # Fill NaN values with -1 (indicating unlabeled)
        all_labels_df['label'] = all_labels_df['label'].fillna(-1).astype(int)

        # Save the labels to a Parquet file
        all_labels_df.to_parquet(output_path, index=False)

        print(f"\nLabeling completed. Labeled {len(labels)} items.")
        print(f"Labels saved to {output_path}")
        print(f"Total rows in labels file: {len(all_labels_df)}")
        print(f"Number of labeled items: {len(labels)}")
        print(f"Number of unlabeled items: {len(all_labels_df) - len(labels)}")
    else:
        print("\nNo items were labeled.")

    # Close the browser if it was initialized
    if browser:
        try:
            print("Closing browser...")
            browser.quit()
            print("Browser closed successfully")
        except Exception as e:
            print(f"Error closing browser: {e}")

def main():
    parser = argparse.ArgumentParser(description='Label items in the html-features.parquet file')
    parser.add_argument('html_features', help='Path to the html-features.parquet file')
    parser.add_argument('json_folder', help='Path to the folder containing JSON files')
    parser.add_argument('output', help='Path to save the labels.parquet file')
    parser.add_argument('--max-domains', type=int, help='Maximum number of distinct domains to label')

    args = parser.parse_args()

    label_items(args.html_features, args.json_folder, args.output, args.max_domains)

if __name__ == '__main__':
    main()
