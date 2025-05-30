# WebCat

Implementation of an LLM-based website classifier, as described in [Thomas Daniels's master thesis](https://assets.dnsbelgium.be/attachment/thesis%20Thomas%20Daniels.pdf).

## Setup

WebCat requires Python 3.10 or above.
Create a new virtual environment to isolate dependencies from other projects:

```
python -m venv .venv
source .venv/bin/activate
```

Install the necessary dependencies: `pip install -r requirements.txt`. For tests and typechecking, also install the dependencies from `requirements-dev.txt`.

A part of the model input consists of the domain name split in words, for which [wordsegment](https://github.com/grantjenks/python-wordsegment) is used. In the `wordsegment/` directory, you still need to create a `unigrams.txt` and `bigrams.txt` file. For English, the files `unigrams.txt` and `bigrams.txt` [from the original library](https://github.com/grantjenks/python-wordsegment/tree/master/wordsegment) can be used. For other languages, they could be derived from datasets such as [Web 1T 5-gram](https://catalog.ldc.upenn.edu/LDC2009T25).

## Setup container

It is possible to run WebCat inside a container. Inside this container a webserver will be started.
To access this webserver the relevant port needs to be exported. It maybe also a good idea to export the folder
which contains the generated parquet files.

You need to install the dependencies from `requirements-webserver.txt` to use the webserver container.

Example:
```shell
# Build the container
docker build -t webcat-server -f Containerfile .

# Start the container 
docker run --rm \
-v ./data/models:/data/models \
-v ./data/hdf5_files:/data/hdf5_files \
-v ./data/parquet_files_in:/data/parquet_files_in \
-v ./data/parquet_files_out:/data/parquet_files_out \
# Uncomment the next line to pass all gpus to into the container
# --gpus all \
-p 8000:8000 \
webcat-server  
```

The webserver has an integrated documentation which is reachable on the path /docs or /redoc.

It will also expose a metrics endpoint which is reachable on the path /metrics.


## Obtaining or constructing a dataset

### From [Mercator](https://github.com/DNSBelgium/mercator)

Set the following environment variables, or fill the `.env` file:

```text
MERCATOR_HOST=...
MERCATOR_PORT=...
MERCATOR_DB=...
MERCATOR_USER=...
MERCATOR_PASS=...
```

Datasets can be loaded from Mercator, using a table (or multiple) that has at least a `visit_id` (UUID) and, for training/testing data, a `label` (text) column.
Training data can be downloaded and saved like this:

```text
python dataset.py train OUT_X OUT_Y TABLE1 [TABLE2 ...]
```

Where `OUT_X` and `OUT_Y` are the paths for the output files, saved in Parquet format.
`TABLE1` is just the name of the table.

Similar for testing data (if a website has multiple labels from different annotators, use one row per label - `visit_id` is not expected to be unique):

```text
python dataset.py test OUT_X OUT_Y TABLE1 [TABLE2 ...]
```

If there are labels in this test set that should be ignored, edit `FETCH_TEST_SET_IGNORE_LABELS` in `config.py`.

Data without label, that can be used for predictions, can be downloaded like this:

```text
python dataset.py predict OUT_X TABLE1 [TABLE2 ...]
```

### Without Mercator

The format of x values is the same for training, testing, and prediction. The format of y values differs between training and testing.

For the x values, construct a Parquet file with the following columns, and a row per training sample:

* `visit_id` - a unique textual identifier for a web page snapshot
* `domain_name` - domain name of the website (including the TLD)
* `body_text` - the document text (title text and body text concatenated - just the text, no HTML)
* `meta_text` - the meta description of the web page, empty string if nonexistent
* `external_hosts` - a `list` of external domain names that the web page links to
* Each numerical feature defined in config.py as `NUMERICAL_FEATURES` (edit this config variable as desired)

For the y values for training, construct a Parquet file with one column, named `label`, containing a string with the label of the web page. The rows must be in the same order as the x values.

For the y values for testing, construct a Parquet file with one column, named `labels`, containing a _list_ of strings with the labels of the web page. This is to support multiple ground truth labels from different human annotators, and a distinction between unanimous and controversial websites is made during evaluation. Even if you have only one label per web page, this column must contain a list. The rows must be in the same order as the x values.

### Mercator Batch JSON Preprocessing

The project includes scripts to convert JSON files to Parquet format, merge multiple Parquet files, and label items for training.

#### Converting JSON to Parquet

To convert JSON files to Parquet format, use the `json_to_parquet.py` script:

```bash
python json_preprocessing/json_to_parquet.py <input_folder> <output_folder>
```

This script will process all JSON files in the input folder and create a separate Parquet file for each JSON file in the output folder. The Parquet files will contain only the HTML features from the JSON files.

You can also use the provided shell script:

```bash
./convert_json_to_parquet.sh
```

This script will convert all JSON files in the `data/json/web` directory to Parquet files in the `data/parquet` directory.

#### Merging Parquet Files

To merge multiple Parquet files into a single file, use the `merge_parquet.py` script:

```bash
python json_preprocessing/merge_parquet.py <input_directory> <output_file>
```

#### Labeling Items

To manually label items for training, use the `label_items.py` script:

```bash
python labeller/label_items.py <html_features_path> <json_folder> <output_path> [--max-domains <max_domains>]
```

This script will load the HTML features from the specified Parquet file, find the corresponding JSON file for each visit_id, open the URL in a web browser, and prompt you to label each item as 0 or 1. The script will automatically skip domains that have already been labeled and apply the previous label to new items with the same domain/URL. The labels will be saved to the specified output path as a Parquet file.

You can use the `--max-domains` parameter to limit the number of distinct domains to label. Once the specified number of domains has been labeled, the script will stop. This is useful for creating smaller labeled datasets or for stopping the labeling process after a certain number of domains have been labeled.

The script uses Selenium WebDriver to reuse the same browser tab for all URLs, which makes the labeling process more efficient. You'll need to install Selenium and a compatible WebDriver (e.g., ChromeDriver) to use this feature:

```bash
pip install selenium
```

If Selenium is not available or encounters an error, the script will fall back to the default webbrowser module, which opens a new tab for each URL.


## Preprocessing

Before training or predicting, the dataset still needs to be preprocessed to obtain inputs that can directly be fed into the model.
The input file(s) is the dataset(s) from the previous step (Parquet files), and the output is an HDF5 file.
The preprocessing step for predictions also requires a trained model.

For training:

```text
python preprocess.py train IN_X IN_Y OUT
```

To specify the training/validation split, use `--split FRACTION`. The default is `0.15`.

For predictions:

```text
python preprocess.py predict IN_X MODEL OUT 
```

## Training

After preprocessing, the training process can be executed with this command, outputting a model file:

```text
python model.py train INPUTS OUT
```

`INPUTS` is the path of the preprocessed data (the output of the previous step).
`OUT` is the path where the model should be saved.

Optional arguments:

* `--batch-size` (default 24)
* `--epochs` (default 4)
* `--learning-rate` (default 2e-5)
* `--seed` (accepts an integer or the string `random`, default `random`)

If your dataset only has two distinct labels, the model is automatically trained as a binary model instead of a multiclass model (which means that AUC-PR instead of F1-score is used during validation, and the prediction output will also include the confidence value between 0 and 1).
The two labels are ordered lexicographically and the first one is taken as the negative label and the second one as positive.
That is not configurable, but it works out fine if label pairs such as `"No"`/`"Yes"`, `"False"`/`"True"`, or `"Negative"`/`"Positive"` are chosen.

## Predictions

```text
python model.py predict DATA MODEL OUT
```

`DATA` is the HDF5 file from the preprocessing step, `MODEL` the path to the trained model, and `OUT` is the path where the predictions should be stored (as a Parquet file).

If the output should include the entropies of the predictions, use the `--entropies` flag.
This is useful to find the most uncertain websites for active learning.

A class distribution of the generated predictions can be printed using:

```text
python predictions.py distribution PREDICTIONS
```

## Testing

Testing the model takes place in two steps:

* Generating predictions on a test set, as explained in the previous section.
* Comparing those predictions to the real labels.

For a multiclass model, this comparison can be done using `model_results.py`:

```text
python model_results.py TRUE_Y MODEL1_PRED [MODEL2_PRED]
```

This prints an overview of the performance of the model.
`TRUE_Y` is the path to the Parquet file containing the true labels.
`MODEL1_PRED` is the result of `model.py predict`.
It's possible to compare the results of two different models by passing the predictions of a second model as well, for the same set of true labels.

This script gives a detailed breakdown of the performance per class.
Pass the `--no-details` flag to only return a summary.

A confusion matrix can be plotted using:

```text
python confusion_matrix.py TRUE_Y MODEL_PRED
```

For a binary model, use `model_results_binary.py` instead:

```text
python model_results_binary.py TRUE_Y MODEL1_PRED [MODEL2_PRED]
```

A precision-recall curve can be plotted using:

```text
python binary_prc.py TRUE_Y MODEL_PRED --plot
```

## Ensembling

The predictions of three models can be combined (by majority vote) like this:

```text
python predictions.py combine OUT P1 P2 P3
```

Note: This assumes that the order of the websites is the same in all three files! (If you used `model.py predict` three times on the same input data, this will be the case.)

If you only want to keep predictions that have a majority (so, delete those where all three models predicted a different class), pass the `--delete-if-no-majority` flag.
This is useful for the self-distillation process, which can be performed using the following steps:

* Train 3 models from the manually labelled training set, with a different seed (or all three randomly seeded).
* Use these models to generate predictions on an unseen set of websites, of the same size as the training set.
* Combine the resulting 3 sets of predictions using `combine` with `--delete-if-no-majority`.
* Use those predictions as additional training data (push them to a Mercator table and re-do the training process starting from "Obtaining or constructing a dataset").

## Active learning

* Generate the entropies of predictions on a large unseen dataset with `model.py predict --entropies`
* Sort the results with `python active_learning.py sort IN OUT [--take N]`, so the most uncertain ones are at the top of the list
* Manually label those visits
* Re-do the training process with the extra labels

## Using a different model (architecture)

To use a different model than the default XLM-RoBERTa BASE model, take these steps:

* Edit the `PRETRAINED_MODEL` variable in `config.py`.
* Change the necessary class names in `xlmr.py` to use the correct implementation from the `transformers` library. This is not needed if you want to use `xlm-roberta-large` instead of `xlm-roberta-base` but it is necessary if you use a different architecture.