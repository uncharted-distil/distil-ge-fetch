#!/bin/bash

# fetch data for central ethiopia - no collection / channels provided so defaults to sentinel-2
python3 fetch_data.py \
    --coverage_file data/shire.geojson \
    --poi_file data/shire_points.geojson \
    --outdir data/shire_test \
    --start_date 2018-01-01 \
    --end_date 2019-02-01 \
    --precision 5 \
    --interval 30 \
    --n_jobs 4 \
    --sampling 1.0 \
    --save_requests

# unzip the imagery into directories named swarm_sighting (points of interest) and
# no_swarm_sighting (area sample)
python3 create_dataset.py \
    --download_location data/shire_test \
    --output_location data/shire_test_sentinel \
    --positive_label swarm_sighting \
    --negative_label no_swarm_sighting

# fetch matching soil moisture data - uses saved requests to ensure the same tile set is generated
python3 fetch_data.py \
    --input_file data/shire_test/requests.json \
    --outdir data/shire_test_moisture \
    --n_jobs 4 \
    --collection ECMWF/ERA5/MONTHLY\
    --channels total_precipitation

# unzip the moisture data into a flat directory structure
python3 create_dataset.py \
    --download_location data/shire_test_moisture \
    --output_location data/shire_test_moisture_sentinel \
    --flatten

