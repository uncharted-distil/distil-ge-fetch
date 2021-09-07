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
import pathlib

from shapely.geometry import geo

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
    polygon = geometry.shape(polygon)
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


# Generates a set of geohashes that are covered by a geojson polygon
def geohashes_from_geojson_poly(coverage_geojson, precision):
    # TODO: better validation
    assert coverage_geojson["type"] == "FeatureCollection"
    polygon = coverage_geojson["features"][0]
    assert polygon["type"] == "Feature"
    assert polygon["geometry"]["type"] == "Polygon"

    # determine geohashes that overlap our AoI
    geohashes_aoi = poly_to_geohashes(
        polygon["geometry"], precision=precision, coarse_precision=precision
    )
    return geohashes_aoi


def geohash_to_array_str(geohash_str):
    # return geohash as a flat list with alternating X,Y values, starting
    # at LL and moving CW
    lat, lon, lat_d, lon_d = geohash.decode_exactly(geohash_str)
    bounds = f"{lon-lon_d},{lat-lat_d},{lon-lon_d},{lat+lat_d},{lon+lon_d},{lat+lat_d},{lon+lon_d},{lat-lat_d}"
    return bounds


# Generates a set of geohashes that are covered by a geojson polygon
def geohashes_from_geojson_points(
    geohashes_aoi, points_geojson, start_date, end_date, precision
):
    assert points_geojson["type"] == "FeatureCollection"
    points = points_geojson["features"]

    start_iso = date.fromisoformat(start_date)
    end_iso = date.fromisoformat(end_date)

    # loop through the points of interest and clip each to the geographic and temporal bounds -  we save a list
    # a list of dates per hash since different points could map to the same bucket at different points
    # in time
    geohash_points = {}
    for point in points:
        point_iso = parse(point["properties"]["date"]).date()

        if point_iso >= start_iso and point_iso <= end_iso:
            assert point["type"] == "Feature"
            assert point["geometry"]["type"] == "Point"
            assert point["properties"] is not None
            assert point["properties"]["date"] is not None
            coords = point["geometry"]["coordinates"]

            gh = geohash.encode(coords[1], coords[0], precision=precision)
            if gh not in geohashes_aoi:
                continue

            if gh not in geohash_points:
                geohash_points[gh] = []
            geohash_points[gh].append(point_iso)

    return geohash_points


# Generates tile fetch requests given an input coverage polygon,
# geohash precision, start and date, and
# interval.
def generate_fetch_requests(
    geohashes_aoi,
    start_date,
    end_date,
    interval_days,
    sampling_rate=1.0,
):
    # determine time intervals
    delta = date.fromisoformat(end_date) - date.fromisoformat(start_date)
    intervals = []
    for i in range(int(delta.days / interval_days)):
        curr_start_date = date.fromisoformat(start_date) + timedelta(
            days=i * interval_days
        )
        curr_end_date = curr_start_date + timedelta(days=interval_days)
        intervals.append((curr_start_date, curr_end_date))

    # create a final tile list
    tile_requests = []
    for gh in geohashes_aoi:
        for interval in intervals:
            tile_requests.append((gh, False, interval))

    # randomly sample the request set
    random.shuffle(tile_requests)
    subset_length = math.floor(len(tile_requests) * sampling_rate)
    tile_requests_sampled = tile_requests[:subset_length]

    unique_hashes = set()
    for r in tile_requests_sampled:
        unique_hashes.add(r[0])
    print(f"unique background tiles: {len(unique_hashes)}")
    print(f"total background tile requests: {len(tile_requests_sampled)}")

    return tile_requests_sampled


# Generates tile fetch requests given an input coverage polygon,
# geohash precision, optional points of interest, start and date, and
# interval.
def generate_fetch_requests_poi(fetch_requests_aoi, geohashes_poi, interval_days):
    # Create records for each geohash and append them to the AoI list
    start_len = len(fetch_requests_aoi)
    fetch_requests = fetch_requests_aoi
    for gh, dates in geohashes_poi.items():
        for d in dates:
            end = d + timedelta(days=interval_days)
            fetch_requests.append((gh, True, (d, end)))

    print(f"total poi requests: {len(fetch_requests) - start_len}")
    print(f"total tile requests (poi + background): {len(fetch_requests)}")

    return fetch_requests


# Earth Engine collection info.

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

    with open(outpath, "w") as outfile:
        json.dump(example_tile_info, outfile, indent=4)
    return


# Fetches data from a URL with support for retries
@backoff.on_exception(backoff.constant, urllib.error.HTTPError, max_tries=4, interval=2)
def safe_urlretrieve(url, outpath):
    _ = urlretrieve(url, outpath)


# Fetch a single tile given request info, collection and bands of interest
def fetch_tile(request, outdir, collection, bands):

    # Set tile output path to combo of geohash the date and extension is added later
    outpath = os.path.join(
        outdir, request["geohash"] + "_" 
    )
    # get the bounding quad for the geohash
    cell = geohashes_to_cells([request["geohash"]])[0]
    # Filter the requested collection by the supplied bands and start/end dates
    # for the request.
    filtered_collection = (
        ee.ImageCollection(collection)
        .select(bands)
        .filterDate(request["date_start"], request["date_end"])
        .filterBounds(ee.Geometry.Point(cell._coordinates[0][0])) # filter by top left point of quad (note: this is an intersection filter)
        .filterBounds(ee.Geometry.Point(cell._coordinates[0][2])) # filter by bottom right point of quad (note: this is an intersection filter)
        # the result of the two filterBounds is a tile the contains all of our quad
    )

    # Apply additional cloud filtering for sentinel-2 tiles.
    if collection == SENTINEL_2_COLLECTION:
        filtered_collection = filtered_collection.filter(
            ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10)
        ).map(
            mask_s2_clouds, True
        )  # Apply cloud mask
    # here we are getting the bands getInfo() is an api call 
    count = filtered_collection.count().getInfo()
    # if there is no bands returned then there are no images for this specific geohash given the constraints above
    if not len(count['bands']):
        print("missing images for: ", [request["geohash"]])
        return
    # grab the first tile from the collection which should be the earliest tile
    first_image = filtered_collection.first()
    # grab the tiles actual date that it was taken
    image_date = first_image.date().format("yyyy-MM-dd").getInfo()
    # apply date and file extension to the path
    outpath += image_date + ".zip"
    # clip the tile, we are looking for a consistent tile size so clipping is necessary
    clipped_image = first_image.clip(cell)
    # Generate a URL from the cell image and download
    try:
        url = clipped_image.getDownloadURL(
            params={"name": request["geohash"], "crs": "EPSG:4326", "scale": 10}
        )
        _ = safe_urlretrieve(url, outpath)
    except:
        pass
