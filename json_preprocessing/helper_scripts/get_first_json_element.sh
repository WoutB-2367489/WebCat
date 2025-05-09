#!/bin/bash

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install it using your package manager."
    echo "For example: sudo apt-get install jq"
    exit 1
fi

# Function to process JSON data
process_json() {
    # Check if the input is an array and get the first element
    # If it's not an array, return the entire object
    jq 'if type == "array" and length > 0 then .[0] else . end'
}

# Check if data is being piped in
if [ -t 0 ]; then
    # No data is being piped in, check if a file is provided
    if [ $# -eq 0 ]; then
        echo "Usage: $0 <json_file>"
        echo "Or pipe JSON data: cat file.json | $0"
        exit 1
    fi

    # If a file is provided, use it as input
    if [ -f "$1" ]; then
        cat "$1" | process_json
    else
        echo "Error: File '$1' not found."
        exit 1
    fi
else
    # Data is being piped in
    process_json
fi
