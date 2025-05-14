#!/bin/bash

# Variables
input_folder=data/json/web
parquet_folder=data/parquet
features_parquet_file=data/html_features.parquet
labels_parquet_file=data/labels.parquet
filtered_foleder=data/filtered
domains_labeled=20 # 20 domains won't do. Just as poc.
model_date=$(date '+%Y-%m-%d_%H-%M-%S')
preprocessed_training_data=data/preprocessed/preprocessed_training_${model_date}.HDF5
model_file=models/model_${model_date}.HDF5
predictions_file=data/predictions_${model_date}.parquet
preprocessed_prediction_data=data/preprocessed/preprocessed_prediction_${model_date}.HDF5

mkdir -p models
mkdir -p data/preprocessed
mkdir -p $parquet_folder
mkdir -p $filtered_foleder


#echo "__________JSON TO PARQUET___________"
#python json_preprocessing/json_to_parquet.py $input_folder "data/parquet/"



#echo "__________MERGE PARQUET___________"
#python json_preprocessing/merge_parquet.py $parquet_folder $features_parquet_file


#echo "__________LABEL DOMAINS___________"
#python labeller/label_items.py $features_parquet_file $input_folder $labels_parquet_file --max-domains $domains_labeled


echo "__________FILTER UNLABELED RECORDS___________"
python json_preprocessing/filter_labeled_data.py --html-features $features_parquet_file --labels $labels_parquet_file --output-dir $filtered_foleder


echo "__________PREPROCESS TRAINING DATA___________"
python preprocess.py train --split 0.15 ${filtered_foleder}/filtered-html-features.parquet ${filtered_foleder}/filtered-labels.parquet ${preprocessed_training_data}


echo "__________TRAIN MODEL___________"
python model.py train --batch-size 8 --epochs 4 --learning-rate 2e-5 --seed "random" ${preprocessed_training_data} ${model_file}


echo "__________PREPROCESS FOR PREDICTION___________"
python preprocess.py predict ${features_parquet_file} ${model_file} ${preprocessed_prediction_data}


echo "__________MAKE PREDICTIONS___________" # --entropies: Include prediction entropies in output (useful for active learning)
python model.py predict --entropies ${preprocessed_prediction_data} ${model_file} ${predictions_file}


