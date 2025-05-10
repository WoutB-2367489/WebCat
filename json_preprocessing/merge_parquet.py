#!/usr/bin/env python3
import os
import sys
import glob
import argparse
import pandas as pd

def merge_parquet_files(input_dir, output_file):
    """
    Merge all Parquet files in a directory into a single Parquet file.
    
    Parameters:
    -----------
    input_dir : str
        Path to the directory containing Parquet files
    output_file : str
        Path to the output Parquet file
    
    Returns:
    --------
    int
        Number of rows in the merged file
    """
    # Ensure input_dir ends with a slash
    if not input_dir.endswith('/'):
        input_dir += '/'
    
    # Check if directory exists
    if not os.path.isdir(input_dir):
        raise ValueError(f"Directory not found: {input_dir}")
    
    # Get all Parquet files in the directory
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))
    
    if not parquet_files:
        raise ValueError(f"No Parquet files found in {input_dir}")
    
    print(f"Found {len(parquet_files)} Parquet files in {input_dir}")
    
    # Create a list to store DataFrames
    dfs = []
    
    # Read each Parquet file and append to the list
    for i, parquet_file in enumerate(parquet_files):
        print(f"Processing file {i + 1}/{len(parquet_files)}: {os.path.basename(parquet_file)}")
        df = pd.read_parquet(parquet_file)
        dfs.append(df)
    
    # Concatenate all DataFrames
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Write the merged DataFrame to a Parquet file
    merged_df.to_parquet(output_file, index=False)
    
    print(f"Merged {len(parquet_files)} Parquet files into {output_file}")
    print(f"Total rows in merged file: {len(merged_df)}")
    
    return len(merged_df)

def main():
    parser = argparse.ArgumentParser(description='Merge all Parquet files in a directory into a single Parquet file')
    parser.add_argument('input_dir', help='Path to the directory containing Parquet files')
    parser.add_argument('output_file', help='Path to the output Parquet file')
    
    args = parser.parse_args()
    
    try:
        merge_parquet_files(args.input_dir, args.output_file)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()