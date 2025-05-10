#!/bin/bash

# Create output directory if it doesn't exist
mkdir -p data/merged

# Run the merge script
echo "Starting the merge process..."
python json_preprocessing/merge_parquet.py data/parquet/ data/merged/merged.parquet

# Check if merge was successful
if [ $? -ne 0 ]; then
  echo "Error: Merge process failed"
  exit 1
fi

# Display information about the merged file
if [ -f "data/merged/merged.parquet" ]; then
  echo "Merge completed successfully!"
  echo "Merged file saved to data/merged/merged.parquet"
  
  # Use Python to display information about the merged file
  python -c "import pandas as pd; df = pd.read_parquet('data/merged/merged.parquet'); print(f'Number of rows: {len(df)}'); print(f'Number of columns: {len(df.columns)}'); print(f'First few rows:'); print(df.head())"
else
  echo "Error: Merged file not found"
  exit 1
fi

echo "Test completed successfully!"