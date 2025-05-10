#!/bin/bash

# Make sure the script is executable
chmod +x json_preprocessing/query_parquet.py

echo "Testing query_parquet.py script..."

# Test 1: Basic query - count the total number of rows
echo "Test 1: Counting total rows across all Parquet files"
python json_preprocessing/query_parquet.py data/parquet/ --query "SELECT COUNT(*) AS total_rows FROM combined_data"

# Test 2: Query with filtering - count rows for a specific domain
echo -e "\nTest 2: Counting rows for domains containing 'example'"
python json_preprocessing/query_parquet.py data/parquet/ --query "SELECT domain_name, COUNT(*) AS count FROM combined_data WHERE domain_name LIKE '%example%' GROUP BY domain_name"

# Test 3: Query with aggregation - average number of images per domain
echo -e "\nTest 3: Average number of images per domain"
python json_preprocessing/query_parquet.py data/parquet/ --query "SELECT domain_name, AVG(nb_imgs) AS avg_images FROM combined_data GROUP BY domain_name ORDER BY avg_images DESC LIMIT 5"

# Test 4: Save results to a file
echo -e "\nTest 4: Saving results to a file"
python json_preprocessing/query_parquet.py data/parquet/ --query "SELECT visit_id, domain_name, nb_imgs, nb_links_int, nb_links_ext FROM combined_data LIMIT 10" --output test_output.csv
echo "Results saved to test_output.csv"
cat test_output.csv

# Test 5: Output in JSON format
echo -e "\nTest 5: Output in JSON format"
python json_preprocessing/query_parquet.py data/parquet/ --query "SELECT visit_id, domain_name FROM combined_data LIMIT 3" --format json

echo -e "\nAll tests completed!"