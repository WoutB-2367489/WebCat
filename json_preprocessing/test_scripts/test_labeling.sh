#!/bin/bash

# Create output directory if it doesn't exist
mkdir -p parquet_output

# Process a small subset of JSON files for testing
echo "Creating a test Parquet file..."
python json_preprocessing/json_to_parquet.py data/json/web parquet_output/test_labeling.parquet

# Check if conversion was successful
if [ $? -ne 0 ]; then
  echo "Error: Failed to create test Parquet file"
  exit 1
fi

# Run the labeling function
echo "Starting the labeling process..."
python json_preprocessing/json_to_parquet.py data/json/web parquet_output/test_labeling.parquet --label --labels-output parquet_output/test_labels.json

# Check if labeling was successful
if [ $? -ne 0 ]; then
  echo "Error: Labeling process failed"
  exit 1
fi

# Display the labels if they exist
if [ -f "parquet_output/test_labels.json" ]; then
  echo "Labeling completed successfully!"
  echo "Labels saved to parquet_output/test_labels.json"
  echo "Sample of labels:"
  head -n 10 parquet_output/test_labels.json
else
  echo "Error: Labels file not found"
  exit 1
fi

echo "Test completed successfully!"