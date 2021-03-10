"""
Utilities for computing geohashes and fetching data from Google Earth Engine.  Copied
from https://github.com/cfld/locusts/, with a small amount of cleanup done.
"""

import os
from shapely import geometry
from itertools import product
import geohash
from polygon_geohasher.polygon_geohasher import (
    polygon_to_geohashes,
    geohashes_to_polygon,
)
from datetime import date, timedelta
from dateutil.parser import parse
import ee
import backoff
import random
import urllib
from urllib.request import urlretrieve
import json
import math

GEOHASH_CHARACTERS = [
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "j",
    "k",
    "m",
    "n",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
]


# Converts a polygon into a list of intersected / contained geohashes
def poly_to_geohashes(polygon, precision=6, coarse_precision=None, inner=True):
    polygon = geometry.shape(polygon.toGeoJSON())
    if coarse_precision is None:
        geohashes = polygon_to_geohashes(polygon, precision=precision, inner=inner)
    else:
        geohashes = polygon_to_geohashes(
            polygon, precision=coarse_precision, inner=inner
        )

        curr_precision = coarse_precision
        while curr_precision < precision:
            geohashes = [a + b for a, b in product(geohashes, GEOHASH_CHARACTERS)]
            curr_precision += 1

    return sorted(list(geohashes))


# Converts a list of geohashes into their representative bounding boxes expressed
# as Earth Engine geometry
def geohashes_to_cells(geohashes):
    cells = [geohashes_to_polygon([h]) for h in geohashes]
    cells = [ee.Geometry(geometry.mapping(c)) for c in cells]
    return cells


# Generates tile fetch requests given an input coverage polygon,
# geohash precision, start and date, and
# interval.
def generate_fetch_requests(
    coverage_geojson,
    precision,
    start_date,
    end_date,
    interval_days,
    sampling_rate=1.0,
):
    # TODO: better validation
    assert coverage_geojson["type"] == "FeatureCollection"
    polygon = coverage_geojson["features"][0]
    assert polygon["type"] == "Feature"
    assert polygon["geometry"]["type"] == "Polygon"

    # determine geohashes that overlap our AoI
    geom_polygon = ee.Geometry.Polygon(polygon["geometry"]["coordinates"])
    geohashes_aoi = poly_to_geohashes(
        geom_polygon, precision=precision, coarse_precision=precision
    )

    # randomly sample a subset
    random.shuffle(geohashes_aoi)
    subset_length = math.floor(len(geohashes_aoi) * sampling_rate)
    geohashes_sampled = geohashes_aoi[:subset_length]

    # generate request times
    return generate_interval_requests(
        geohashes_sampled, start_date, end_date, interval_days
    )


# Generates tile fetch requests given an input coverage polygon,
# geohash precision, optional points of interest, start and date, and
# interval.
def generate_fetch_requests_poi(
    coverage_geojson,
    poi_geojson,
    precision,
    start_date,
    end_date,
    interval_days=30,
    poi_expansion=1,
    sampling_rate=1.0,
):
    # TODO: better validation
    assert coverage_geojson["type"] == "FeatureCollection"
    polygon = coverage_geojson["features"][0]
    assert polygon["type"] == "Feature"
    assert polygon["geometry"]["type"] == "Polygon"

    # determine geohashes that overlap our AoI
    geom_polygon = ee.Geometry.Polygon(polygon["geometry"]["coordinates"])
    geohashes_aoi = poly_to_geohashes(
        geom_polygon, precision=precision, coarse_precision=precision
    )

    assert poi_geojson["type"] == "FeatureCollection"

    points = poi_geojson["features"]

    start_iso = date.fromisoformat(start_date)
    end_iso = date.fromisoformat(start_date)

    geohash_points = set()
    # loop through the points of interest and clip each to the geographic and time bounds
    for point in points:
        assert point["type"] == "Feature"
        assert point["geometry"]["type"] == "Point"
        assert point["properties"] is not None
        assert point["properties"]["date"] is not None

        # point_date = parse(point["properties"]["date"])
        point_iso = date.fromisoformat(point["properties"]["date"])
        if point_iso >= start_iso and point_iso <= end_iso:
            coords = point["geometry"]["coordinates"]
            geohash_points.add(
                geohash.encode(coords[1], coords[0], precision=precision)
            )
    clipped_geohashes = geohash_points.intersection(geohashes_aoi)

    # expand outward N steps
    expansion_geohashes = set(clipped_geohashes)
    for _ in range(poi_expansion):
        for geo_hash in list(expansion_geohashes):
            expansion_geohashes |= set(geohash.expand(geo_hash))

    # clip to geobounds again
    clipped_expansion_geohashes = list(expansion_geohashes.intersection(geohashes_aoi))

    # grab a random subset of the data and merge it with the original set
    random.shuffle(clipped_expansion_geohashes)
    subset_length = math.floor(len(clipped_expansion_geohashes) * sampling_rate)
    clipped_expansion_geohashes = clipped_expansion_geohashes[:subset_length]
    clipped_expansion_geohashes.extend(list(clipped_geohashes))
    final_geohashes = list(set(clipped_expansion_geohashes))

    # generate requests
    return generate_interval_requests(
        final_geohashes, start_date, end_date, interval_days
    )


def generate_interval_requests(geohashes, start_date, end_date, interval_days):
    fetch_requests = []

    # generate request times
    delta = date.fromisoformat(end_date) - date.fromisoformat(start_date)
    for i in range(int(delta.days / interval_days)):
        curr_start_date = date.fromisoformat(start_date) + timedelta(
            days=i * interval_days
        )
        curr_end_date = curr_start_date + timedelta(days=interval_days)

        # Create records for each geohash
        for geohash in geohashes:
            fetch_requests.append(
                {
                    "date_start": str(curr_start_date),
                    "date_end": str(curr_end_date),
                    "geohash": geohash,
                }
            )

    return fetch_requests


# Earth Engine collection info.  A more flexible design

# Sentinel collection name
SENTINEL_2_COLLECTION = "COPERNICUS/S2"

# Sentinel-2 target channels
SENTINEL_2_CHANNELS = [
    "B1",
    "B2",
    "B3",
    "B4",
    "B5",
    "B6",
    "B7",
    "B8",
    "B8A",
    "B9",
    "B11",
    "B12",
    "QA60",
]

# Copernicus land cover collection
COPERNICUS_LAND_COVER_COLLECTION = "COPERNICUS/Landcover/100m/Proba-V-C3/Global"

# Copernicus land cover target channels
COPERNICUS_LAND_COVER_CHANNELS = ["discrete_classification"]

# Generates an image
def mask_s2_clouds(image):
    qa = image.select("QA60")

    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11

    mask = qa.bitwiseAnd(cloudBitMask).eq(0)
    mask = mask.bitwiseAnd(cirrusBitMask).eq(0)

    return image.updateMask(mask)


# Write the metadata from the first tile in the collection out
def fetch_metadata(outdir, collection, bands):
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    outpath = os.path.join(outdir, "metadata.json")
    dataset = ee.ImageCollection(collection).select(bands)
    example_tile_info = dataset.toList(1).get(0).getInfo()
    # example_tile_info = dataset.toList(1).get(0).getInfo()
    with open(outpath, "w") as outfile:
        json.dump(example_tile_info, outfile, indent=4)
    return


# Fetches data from a URL with support for retries
@backoff.on_exception(backoff.constant, urllib.error.HTTPError, max_tries=4, interval=2)
def safe_urlretrieve(url, outpath):
    _ = urlretrieve(url, outpath)


# Fetch a single tile given request info, collection and bands of interest
def fetch_tile(loc, outdir, collection, bands):
    # Set tile output path to combo of geohash and start date. Example:
    # scu6k_2015-12-31.zip
    outpath = os.path.join(
        outdir, loc["geohash"] + "_" + str(loc["date_start"]) + ".zip"
    )

    # Filter the requested collection by the supplied bands and start/end dates
    # for the request.
    filtered_collection = (
        ee.ImageCollection(collection)
        .select(bands)
        .filterDate(loc["date_start"], loc["date_end"])
    )

    # Apply additional cloud filtering for sentinel-2 tiles.
    if collection == SENTINEL_2_COLLECTION:
        filtered_collection = filtered_collection.filter(
            ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20)
        ).map(
            mask_s2_clouds
        )  # Apply cloud mask

    # Generate a single image for the collection and clip it to our geohash
    cell = geohashes_to_cells([loc["geohash"]])[0]
    cell_image = (
        filtered_collection.sort("system:index", opt_ascending=False)
        .mosaic()
        .clip(cell)
    )

    # Generate a URL from the cell image and download
    try:
        url = cell_image.getDownloadURL(
            params={"name": loc["geohash"], "crs": "EPSG:4326", "scale": 10}
        )
        _ = safe_urlretrieve(url, outpath)
    except:
        pass
