#!/bin/bash

mkdir -p data/parquet

python json_preprocessing/json_to_parquet.py "data/json/web" "data/parquet/"

