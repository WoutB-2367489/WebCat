#!/bin/bash

# Variables
input_folder=data_cc/json/tls
parquet_folder=data_cc/parquet/tls
tls_features_parquet_file=data_cc/cc_tls_features.parquet
mkdir -p $parquet_folder

echo "__________TLS JSON TO PARQUET___________"
python json_preprocessing/tls_json_to_parquet.py $input_folder "$parquet_folder/"

echo "__________MERGE TLS PARQUET___________"
python json_preprocessing/merge_parquet.py $parquet_folder $tls_features_parquet_file

echo "TLS data processing completed. Output file: $tls_features_parquet_file"