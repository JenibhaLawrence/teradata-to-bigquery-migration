#!/bin/bash
# Uploads files from Linux to GCS

SOURCE_DIR="/opt/data/input"
BUCKET="gs://my-bucket/input"

for file in $SOURCE_DIR/*.csv; do
    echo "Uploading $file ..."
    gsutil cp "$file" "$BUCKET/"
done

echo "Upload completed."
