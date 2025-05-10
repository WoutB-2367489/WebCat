#!/usr/bin/env python3
import os
import pandas as pd
import argparse

def filter_labeled_data(html_features_path, labels_path, output_dir):
    """
    Filter html-features.parquet and labels.parquet to keep only rows with a valid label (not -1).

    Parameters:
    -----------
    html_features_path : str
        Path to the html-features.parquet file
    labels_path : str
        Path to the labels.parquet file
    output_dir : str
        Directory to save the filtered parquet files

    Returns:
    --------
    tuple
        Paths to the filtered html-features and labels parquet files
    """
    # print(f"Loading html-features from: {html_features_path}")
    html_features = pd.read_parquet(html_features_path)

    # print(f"Loading labels from: {labels_path}")
    labels = pd.read_parquet(labels_path)

    # print(f"Original html-features shape: {html_features.shape}")
    # print(f"Original labels shape: {labels.shape}")

    # Check if visit_id is in both dataframes
    if 'visit_id' not in html_features.columns:
        raise ValueError("visit_id column not found in html-features.parquet")

    if 'visit_id' not in labels.columns:
        raise ValueError("visit_id column not found in labels.parquet")

    # Check for rows with label = -1
    if 'label' not in labels.columns:
        raise ValueError("label column not found in labels.parquet")

    invalid_rows = labels[labels['label'] == -1]
    print(f"Number of rows with label = -1: {len(invalid_rows)}")
    print(f"Number of rows with valid labels: {len(labels) - len(invalid_rows)}")

    # Filter out rows where label is -1
    valid_labels = labels[labels['label'] != -1]

    # Get the set of valid visit_ids
    valid_visit_ids = set(valid_labels['visit_id'])

    # Filter html_features to keep only rows with valid visit_ids
    filtered_html_features = html_features[html_features['visit_id'].isin(valid_visit_ids)]

    # print(f"Filtered html-features shape: {filtered_html_features.shape}")
    # print(f"Filtered labels shape: {valid_labels.shape}")

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Remove visit_ids as the last step
    valid_labels = valid_labels.drop(columns=['visit_id'])

    # Save filtered dataframes to parquet files
    html_features_output = os.path.join(output_dir, 'filtered-html-features.parquet')
    labels_output = os.path.join(output_dir, 'filtered-labels.parquet')

    print(f"Saving filtered html-features to: {html_features_output}")
    filtered_html_features.to_parquet(html_features_output, index=False)

    print(f"Saving filtered labels to: {labels_output}")
    valid_labels.to_parquet(labels_output, index=False)

    return html_features_output, labels_output

def main():
    parser = argparse.ArgumentParser(description='Filter parquet files to keep only rows with valid labels (not -1)')
    parser.add_argument('--html-features', default='./data/html-features.parquet',
                        help='Path to the html-features.parquet file')
    parser.add_argument('--labels', default='./data/labels.parquet',
                        help='Path to the labels.parquet file')
    parser.add_argument('--output-dir', default='./data/filtered',
                        help='Directory to save the filtered parquet files')

    args = parser.parse_args()

    try:
        html_features_output, labels_output = filter_labeled_data(
            args.html_features, args.labels, args.output_dir
        )
        print(f"\nFiltering completed successfully!")
        print(f"Filtered html-features saved to: {html_features_output}")
        print(f"Filtered labels saved to: {labels_output}")
    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0

if __name__ == '__main__':
    exit(main())
