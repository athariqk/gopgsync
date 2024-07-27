#!/bin/bash

set -e

echo "Starting deployment..."

# Define variables
FROM_PATH="build_output/"
TO_PATH="${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"

# Ensure the remote path exists
ssh "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ${REMOTE_PATH}"

# Sync the build artifacts
rsync -avzhe ssh "$FROM_PATH" "$TO_PATH"

echo "Deployment complete."
