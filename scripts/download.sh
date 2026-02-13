#!/bin/bash

# Create download directory
mkdir -p downloads/adsb_artifacts

# Repository from the workflow comment
REPO="ggman12/OpenAirframes"

# Get last 15 runs of the workflow and download matching artifacts
gh run list \
  --repo "$REPO" \
  --workflow adsb-to-aircraft-multiple-day-run.yaml \
  --limit 15 \
  --json databaseId \
  --jq '.[].databaseId' | while read -r run_id; do
  
  echo "Checking run ID: $run_id"
  
  # List artifacts for this run using the API
  # Match pattern: openairframes_adsb-YYYY-MM-DD-YYYY-MM-DD (with second date)
  gh api \
    --paginate \
    "repos/$REPO/actions/runs/$run_id/artifacts" \
    --jq '.artifacts[] | select(.name | test("^openairframes_adsb-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{4}-[0-9]{2}-[0-9]{2}$")) | .name' | while read -r artifact_name; do
    
    echo "  Downloading: $artifact_name"
    gh run download "$run_id" \
      --repo "$REPO" \
      --name "$artifact_name" \
      --dir "downloads/adsb_artifacts/$artifact_name"
  done
done

echo "Download complete! Files saved to downloads/adsb_artifacts/"