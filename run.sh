#!/usr/bin/env -S bash --login
set -euo pipefail
# This script is the one that is called by the DPS.
# Use this script to prepare input paths for any files
# that are downloaded by the DPS and outputs that are
# required to be persisted

# Get current location of build script
basedir=$(dirname "$(readlink -f "$0")")

# Create output directory to store outputs.
# The name is output as required by the DPS.
# Note how we dont provide an absolute path
# but instead a relative one as the DPS creates
# a temp working directory for our code.

mkdir -p output


# DPS downloads all files provided as inputs to
# this directory called input.
# In our example the image will be downloaded here.
INPUT_DIR=input
OUTPUT_DIR=output

# Parse positional arguments (4 required, 2 optional)
if [[ $# -lt 4 ]] || [[ $# -gt 6 ]]; then
    echo "Error: Expected 4-6 arguments, got $#"
    echo "Usage: $0 <start_datetime> <end_datetime> <mgrs_tile> <points_href> [id_col] [bands]"
    echo "  bands: space-separated list of band names (e.g., 'red green blue')"
    exit 1
fi

start_datetime="$1"
end_datetime="$2"
mgrs_tile="$3"
points_href="$4"
id_col="${5:-}"
bands="${6:-}"

# Call the script using the absolute paths
# Use the updated environment when calling 'uv run'
# This lets us run the same way in a Terminal as in DPS
# Any output written to the stdout and stderr streams will be automatically captured and placed in the output dir

# unset PROJ env vars
unset PROJ_LIB
unset PROJ_DATA

# Build the command with required arguments
cmd=(
    uv run --no-dev "${basedir}/main.py"
    --start_datetime "${start_datetime}"
    --end_datetime "${end_datetime}"
    --mgrs_tile "${mgrs_tile}"
    --points_href "${points_href}"
)

# Add optional id_col if provided
if [[ -n "${id_col}" ]]; then
    cmd+=(--id_col "${id_col}")
fi

# Add optional bands if provided
if [[ -n "${bands}" ]]; then
    # Split bands on spaces and add each as a separate --bands argument
    for band in ${bands}; do
        cmd+=(--bands "${band}")
    done
fi

# Add output directory and direct bucket access
cmd+=(--output_dir="${OUTPUT_DIR}" --direct_bucket_access)

# Execute the command with UV_PROJECT environment variable
UV_PROJECT="${basedir}" "${cmd[@]}"
