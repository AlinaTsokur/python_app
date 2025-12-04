#!/bin/bash

# Check if a message was provided
if [ -z "$1" ]; then
  echo "Usage: ./save_version.sh \"Your commit message\""
  echo "Example: ./save_version.sh \"Fixed bug in X-RAY report\""
  exit 1
fi

# Add all changes
git add .

# Commit with the provided message
git commit -m "$1"

echo "✅ Version saved successfully!"
