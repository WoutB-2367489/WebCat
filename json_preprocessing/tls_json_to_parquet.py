import os
import sys
import duckdb
import argparse
import glob

parser = argparse.ArgumentParser(description='Convert TLS JSON files from a folder to Parquet files using DuckDB')
parser.add_argument('input_folder', help='Path to the folder containing TLS JSON files')
parser.add_argument('output_parquet', help='Path to the output Parquet folder')
args = parser.parse_args()

input_folder = args.input_folder
output_parquet_folder = args.output_parquet

con = duckdb.connect(database=':memory:')

json_files = glob.glob(os.path.join(input_folder, "*.json"))

if not json_files:
    print(f"No JSON files found in {input_folder}")
    sys.exit(1)

print(f"Found {len(json_files)} JSON files in {input_folder}")

for i, json_file_path in enumerate(json_files):

    print(f"Processing file {i + 1}/{len(json_files)}: {json_file_path}")

    base_filename = os.path.splitext(os.path.basename(json_file_path))[0]

    if not output_parquet_folder.endswith('/'):
        output_parquet_folder += '/'

    output_file = f"{output_parquet_folder}{base_filename}.parquet"

    con.execute(f"""
        COPY (
            SELECT 
                domain_data.visit_id,
                domain_data.domain_name,
                unnest.host_name,
                unnest.host_name_matches_certificate,
                unnest.chain_trusted_by_java_platform,
                unnest.certificate_expired,
                unnest.certificate_too_soon,
                unnest.crawl_timestamp,
                unnest.full_scan_entity.crawl_timestamp AS scan_timestamp,
                unnest.full_scan_entity.ip,
                unnest.full_scan_entity.server_name,
                unnest.full_scan_entity.connect_ok,
                unnest.full_scan_entity.support_tls_1_3,
                unnest.full_scan_entity.support_tls_1_2,
                unnest.full_scan_entity.support_tls_1_1,
                unnest.full_scan_entity.support_tls_1_0,
                unnest.full_scan_entity.support_ssl_3_0,
                unnest.full_scan_entity.support_ssl_2_0,
                unnest.full_scan_entity.selected_cipher_tls_1_3,
                unnest.full_scan_entity.selected_cipher_tls_1_2,
                unnest.full_scan_entity.selected_cipher_tls_1_1,
                unnest.full_scan_entity.selected_cipher_tls_1_0,
                unnest.full_scan_entity.selected_cipher_ssl_3_0,
                unnest.full_scan_entity.lowest_version_supported,
                unnest.full_scan_entity.highest_version_supported,
                unnest.full_scan_entity.error_tls_1_3,
                unnest.full_scan_entity.error_tls_1_2,
                unnest.full_scan_entity.error_tls_1_1,
                unnest.full_scan_entity.error_tls_1_0,
                unnest.full_scan_entity.error_ssl_3_0,
                unnest.full_scan_entity.error_ssl_2_0,
                unnest.full_scan_entity.millis_tls_1_3,
                unnest.full_scan_entity.millis_tls_1_2,
                unnest.full_scan_entity.millis_tls_1_1,
                unnest.full_scan_entity.millis_tls_1_0,
                unnest.full_scan_entity.millis_ssl_3_0,
                unnest.full_scan_entity.millis_ssl_2_0,
                unnest.certificate_chain_fingerprints
            FROM read_json_auto('{json_file_path}') AS domain_data,
            UNNEST(domain_data.visits) AS unnest) 
            TO '{output_file}' (FORMAT PARQUET);
    """)

print(f"Conversion completed. All JSON files have been converted to separate Parquet files in: {output_parquet_folder}")

parquet_files = glob.glob(os.path.join(output_parquet_folder, "*.parquet"))
print(f"Total number of Parquet files created: {len(parquet_files)}")

if parquet_files:
    first_parquet = parquet_files[0]
    columns = con.execute(f"SELECT * FROM '{first_parquet}' LIMIT 0").description
    column_names = [col[0] for col in columns]
    print(f"Columns in the Parquet files: {column_names}")

    total_rows = 0
    for parquet_file in parquet_files:
        row_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchone()[0]
        total_rows += row_count

    print(f"Total rows across all Parquet files: {total_rows}")
