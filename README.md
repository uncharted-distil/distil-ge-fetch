# ge-fetch
Python script for fetching Google Earth Engine tiles based on an area of interest, geohash level, and start / end time.  Can also fetch based on an input JSON file.  Note that an activated Google Earth Engine account is required.

Example usage for fetch:

```bash
python3 fetch_data.py \
    --input_file data/sembete.geojson \
    --outdir /data/remote_sensing/sentinel_2/sembete \
    --start_date 2019-01-01 \
    --end_date 2020-01-31 \
    --precision 5 \
    --collection COPERNICUS/S2 \
    --interval 7
```

The end result is a series of tiles downloaded and named according to their band and date. Example:

```bash
/data/remote_sensing/sentinel_2/scu6k_2019-12-30.zip
```

A script is also included to unzip the fetched archives and name them according to the sentinel-2 standard:

```bash
 python3 unzip_downloaded.py --download_location /data/remote_sensing/sentinel_2/sembete --output_location /data/remote_sensing/sentinel_2/sembete_unzipped
```
