#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# List all region codes
regions=("CA" "DE" "FR" "GB" "IN" "JP" "KR" "MX" "RU" "US")

# Target bucket name
BUCKET="yt-bronze-ap-southeast-1-dev"

for region in "${regions[@]}"; do
  # Convert region code to lowercase for the S3 key path (e.g., CA -> ca)
  region_lower=$(echo "$region" | tr '[:upper:]' '[:lower:]')

  echo "Uploading data for region: $region..."

  # Copy CSV files from the 'data' folder at the root
  aws s3 cp "data/${region}videos.csv" "s3://${BUCKET}/raw_statistics/region=${region_lower}/"

  # Copy JSON reference files from the 'data' folder at the root
  aws s3 cp "data/${region}_category_id.json" "s3://${BUCKET}/raw_statistics_reference_data/region=${region_lower}/"
done

echo "All regions uploaded successfully!"