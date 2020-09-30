# ge-fetch
Python script for fetching Google Earth Engine tiles based on an area of interest, geohash level, and start / end time.  Can also fetch based on an input JSON file.  Note that an activated Google Earth Engine account is required.

Example usage:

```bash
python fetch_data.py --input_json data/ethiopia.geojson --start_date 2020-01-01 --end_date 2020-06-01 --output_dir tile_output
```
