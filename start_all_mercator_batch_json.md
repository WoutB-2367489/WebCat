# WebCat Processing Steps

## Variables
````bash
# Variables
input_folder=data/json/web
parquet_folder=data/parquet
features_parquet_file=data/html_features.parquet
labels_parquet_file=data/labels.parquet
processed_folder=data/filtered
domains_labeled=3
model_date=$(date '+%Y-%m-%d_%H-%M-%S')
preprocessed_training_data=data/preprocessed_training_${model_date}.HDF5
model_file=models/model_${model_date}.HDF5
predictions_file=data/predictions_${model_date}.parquet
preprocessed_prediction_data=data/preprocessed_prediction_${model_date}.HDF5
````

## 1. JSON to Parquet Conversion
```bash
python json_preprocessing/json_to_parquet.py $input_folder "data/parquet/"
```

## 2. Merge Parquet Files
```bash
python json_preprocessing/merge_parquet.py $parquet_folder $features_parquet_file
```

## 3. Label Domains
```bash
python labeller/label_items.py $features_parquet_file $input_folder $labels_parquet_file --max-domains $domains_labeled
```

## 4. Filter Unlabeled Records
```bash
python json_preprocessing/filter_labeled_data.py --html-features $features_parquet_file --labels $labels_parquet_file --output-dir $processed_folder
```

## 5. Preprocess Training Data
```bash
python preprocess.py train --split 0.15 ${processed_folder}/filtered-html-features.parquet ${processed_folder}/filtered-labels.parquet ${preprocessed_training_data}
```

## 6. Train Model
```bash
python model.py train --batch-size 24 --epochs 1 --learning-rate 2e-5 --seed "random" ${preprocessed_training_data} ${model_file}
```

## 7. Preprocess for Prediction
```bash
python preprocess.py predict ${features_parquet_file} ${model_file} ${preprocessed_prediction_data}
```

## 8. Make Predictions
```bash
python model.py predict --entropies ${preprocessed_prediction_data} ${model_file} ${predictions_file}
```