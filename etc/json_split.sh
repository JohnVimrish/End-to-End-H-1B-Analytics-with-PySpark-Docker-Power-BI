#!/bin/bash

# Input JSON file path (modify as needed)
INPUT_FILE="/root/docker_dataset/big_data.json"
OUTPUT_DIR="/root/docker_dataset/json_splits/"
CHUNK_SIZE="500M"  # Modify the chunk size as needed (e.g., 100M, 1G)

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

echo "Step 1: Converting JSON array to NDJSON (Newline Delimited JSON)..."
jq -c '.values[]' "$INPUT_FILE" > "$OUTPUT_DIR/formatted_lines.json"

echo "Step 2: Splitting JSON file into chunks of size $CHUNK_SIZE..."
split -b "$CHUNK_SIZE" "$OUTPUT_DIR/formatted_lines.json" "$OUTPUT_DIR/output_part_"

# Remove intermediate formatted file
rm "$OUTPUT_DIR/formatted_lines.json"

echo "Step 3: Wrapping split files into proper JSON format..."

# Loop through each split file and reformat it
for file in "$OUTPUT_DIR"/big_data_*; do
    echo '{"values": [' > "${file}.json"
    cat "$file" | sed '$!s/$/,/' >> "${file}.json"  # Add commas between JSON objects
    echo ']}' >> "${file}.json"
    rm "$file"  # Remove intermediate split file
    echo "Processed ${file}.json"
done

echo "Splitting and formatting complete. JSON files are stored in $OUTPUT_DIR"

