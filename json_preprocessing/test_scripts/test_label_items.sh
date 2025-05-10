#!/bin/bash

# Create output directory if it doesn't exist
mkdir -p data/parquet

# Run the labeling script
echo "Starting the labeling process..."
python labeller/label_items.py data/parquet/html-features.parquet data/json/web data/parquet/labels.parquet

# Check if labeling was successful
if [ $? -ne 0 ]; then
  echo "Error: Labeling process failed"
  exit 1
fi

# Display the labels if they exist
if [ -f "data/parquet/labels.parquet" ]; then
  echo "Labeling completed successfully!"
  echo "Labels saved to data/parquet/labels.parquet"

  # Use Python to display the first few rows of the labels file
  python -c "import pandas as pd; print(pd.read_parquet('data/parquet/labels.parquet').head())"
else
  echo "Error: Labels file not found"
  exit 1
fi

echo "Test completed successfully!"
