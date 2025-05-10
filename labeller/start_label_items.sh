#!/bin/bash

python labeller/label_items.py data/html-features.parquet data/json/web data/labels.parquet --max-domains 1000
