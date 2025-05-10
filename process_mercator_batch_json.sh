#!/bin/bash

# Variables
input_folder=data/json/web
parquet_folder=data/parquet
features_parquet_file=data/html_features.parquet
labels_parquet_file=data/labels.parquet
processed_folder=data/processed_folder
domains_labeled=3

mkdir -p $parquet_folder
mkdir -p $processed_folder

echo "__________JSON TO PARQUET___________"
python json_preprocessing/json_to_parquet.py $input_folder "data/parquet/"

echo "__________MERGE PARQUET___________"
python json_preprocessing/merge_parquet.py $parquet_folder $features_parquet_file

echo "__________LABEL DOMAINS___________"
python labeller/label_items.py $features_parquet_file $input_folder $labels_parquet_file --max-domains $domains_labeled

echo "__________FILTER UNLABELED RECORDS___________"
python json_preprocessing/filter_labeled_data.py --html-features $features_parquet_file --labels $labels_parquet_file --output-dir $processed_folder

echo "__________FILTER UNLABELED RECORDS___________"
python preprocess.py train data/processed_folder/filtered-html-features.parquet data/processed_folder/filtered-labels.parquet models/


