import os
import sys
import duckdb
import argparse
import glob

parser = argparse.ArgumentParser(description='Convert JSON files from a folder to a single Parquet file using DuckDB')
parser.add_argument('input_folder', help='Path to the folder containing JSON files')
parser.add_argument('output_parquet', help='Path to the output Parquet file')
args = parser.parse_args()

input_folder = args.input_folder

output_parquet_folder = args.output_parquet

con = duckdb.connect(database=':memory:')

json_files = glob.glob(os.path.join(input_folder, "*.json"))

if not json_files:
    print(f"No JSON files found in {input_folder}")
    sys.exit(1)

print(f"Found {len(json_files)} JSON files in {input_folder}")

# Process each JSON file
for i, json_file_path in enumerate(json_files):

    print(f"Processing file {i + 1}/{len(json_files)}: {json_file_path}")

    base_filename = os.path.splitext(os.path.basename(json_file_path))[0]

    if not output_parquet_folder.endswith('/'):
        output_parquet_folder += '/'

    output_file = f"{output_parquet_folder}{base_filename}.parquet"

    con.execute(f"""
        COPY (
            SELECT 
                unnest.visit_id,
                unnest.domain_name,
                unnest.nb_imgs,
                unnest.nb_links_int,
                unnest.nb_links_ext,
                unnest.nb_links_tel,
                unnest.nb_links_email,
                unnest.nb_input_txt,
                unnest.nb_button,
                unnest.nb_meta_desc,
                unnest.nb_meta_keyw,
                unnest.nb_numerical_strings,
                unnest.nb_tags,
                unnest.nb_words,
                unnest.title,
                unnest.htmlstruct,
                unnest.body_text,
                unnest.meta_text,
                unnest.body_text_truncated,
                unnest.meta_text_truncated,
                unnest.title_truncated,
                unnest.nb_letters,
                unnest.nb_distinct_hosts_in_urls,
                unnest.nb_facebook_deep_links,
                unnest.nb_facebook_shallow_links,
                unnest.nb_linkedin_deep_links,
                unnest.nb_linkedin_shallow_links,
                unnest.nb_twitter_deep_links,
                unnest.nb_twitter_shallow_links,
                unnest.nb_currency_names,
                unnest.nb_distinct_currencies,
                unnest.distance_title_final_dn,
                unnest.longest_subsequence_title_final_dn,
                unnest.nb_youtube_deep_links,
                unnest.nb_youtube_shallow_links,
                unnest.nb_vimeo_deep_links,
                unnest.nb_vimeo_shallow_links,
                unnest.body_text_language,
                unnest.body_text_language_2,
                unnest.fraction_words_title_initial_dn,
                unnest.fraction_words_title_final_dn,
                unnest.nb_distinct_words_in_title,
                unnest.distance_title_initial_dn,
                unnest.longest_subsequence_title_initial_dn,
                unnest.external_hosts AS "external_hosts"
            FROM read_json_auto('{json_file_path}'),
            UNNEST(html_features) AS unnest) 
            TO '{output_file}' (FORMAT PARQUET);
    """)

print(f"Conversion completed. All JSON files have been converted to separate Parquet files in: {output_parquet_folder}")

# Get a list of all Parquet files in the output directory
parquet_files = glob.glob(os.path.join(output_parquet_folder, "*.parquet"))
print(f"Total number of Parquet files created: {len(parquet_files)}")

# Check the first Parquet file to get column information
if parquet_files:
    first_parquet = parquet_files[0]
    columns = con.execute(f"SELECT * FROM '{first_parquet}' LIMIT 0").description
    column_names = [col[0] for col in columns]
    print(f"Number of columns in the Parquet files: {column_names.count()}")

    # Count total rows across all Parquet files
    total_rows = 0
    for parquet_file in parquet_files:
        row_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchone()[0]
        total_rows += row_count

    print(f"Total rows across all Parquet files: {total_rows}")
