import argparse
import os
import shutil
import zipfile
import pathlib
import re
from datetime import datetime
import collections
import pandas as pd
import geohash
import math
import helpers
import plotly.express as px

ControlInfo = collections.namedtuple("ControlInfo", ["num_tracks", "date", "duration"])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Converts HYSPLIT Locust model output into a geohashed CSV"
    )
    parser.add_argument("--input_dir", type=str, required=True)
    parser.add_argument("--output_file", type=str, default="./output.csv")
    parser.add_argument("--temp_dir", type=str, default="./temp")

    return parser.parse_args()


def extract_input(input_dir, temp_dir):
    os.makedirs(temp_dir, exist_ok=True)
    p = pathlib.Path(input_dir)

    swarm_ids = []

    zip_files = p.glob("*.zip")
    for zf in zip_files:
        shutil.copy(zf, temp_dir)

        temp_zip_file = os.path.join(temp_dir, zf.name)
        with zipfile.ZipFile(temp_zip_file, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        swarm_ids.append(zf.stem)
    return swarm_ids


def parse_control_files(data_dir, ids):
    # regex to ignore comments on a line
    line_re = re.compile("(.*)#")

    # map to hold extract control info
    swarm_control_info = {}

    p = pathlib.Path(data_dir)
    # find control files for each swarm id
    for id in ids:

        swarm_control_info[id] = []

        control_files = p.glob(f"{id}_CONTROL.*.txt")
        for cf in control_files:
            # open the control file
            with open(cf, "r") as f:
                # strip comments
                lines = f.read().splitlines()
                stripped_lines = []
                for l in lines:
                    m = line_re.match(l)
                    if m:
                        stripped_lines.append(m.group(1).strip())
                    else:
                        stripped_lines.append(l.strip())

                # read in required data
                parsed_date = datetime.strptime(stripped_lines[0], "%y %m %d %H %M")
                number_of_tracks = stripped_lines[1]
                duration_hrs = stripped_lines[2 + int(number_of_tracks)]

            swarm_control_info[id].append(
                ControlInfo(number_of_tracks, parsed_date, duration_hrs)
            )
            swarm_control_info[id].sort(key=lambda d: d.date)

        ctrl_arr = swarm_control_info[id]
    return swarm_control_info


def parse_trajectory_files(data_dir, ids, control_info):
    day_re = re.compile(".*_day(\d+)")

    p = pathlib.Path(data_dir)

    # list of trajectory dataframes
    trajectories = []

    # find control files for each swarm id
    for id in ids:
        trajectory_data = {}

        trajectory_files = p.glob(f"{id}_day*.txt")
        for tf in trajectory_files:
            # extract the day and use that to look up the control info
            day = int(day_re.match(str(tf)).group(1))
            ci = control_info[id][day - 1]

            # load csv
            tdf = pd.read_csv(tf, names=["point_id", "x", "y", "altitude"])

            # get rid of the end marker
            tdf = tdf[tdf["point_id"] != "END"]
            # ID field consists of a track number in the first digit, and a record
            # number in the remaining.  They need to be split out.
            tdf["track"] = tdf["point_id"].str.strip().str[0]
            tdf["point_id"] = tdf["point_id"].str.strip().str[1:]
            tdf["date"] = ci.date

            # Group by the track ID number.
            track_groups = tdf.groupby("track")

            # Collect the grouped data frames by track/day
            for track, frame in track_groups:
                if track not in trajectory_data:
                    trajectory_data[track] = []
                trajectory_data[track].append(frame)

        # Compose the data for each track into a single df spanning multiple days
        for _, d in trajectory_data.items():
            tdf = pd.concat(d)
            # recompute index/point ids to enmerate final point orderings
            tdf = tdf.sort_values(["date", "point_id"])
            tdf = tdf.reset_index(drop=True)
            tdf["point_id"] = tdf.index

            # write the swarm id and the starting altitude in for each
            tdf["swarm_id"] = id
            tdf["altitude_id"] = int(tdf.loc[0, "altitude"])

            # save out the final dataframe
            trajectories.append(tdf)

    return trajectories


def geohash_cell_size(level):
    bounds = geohash.bbox("0" * level)
    return (bounds["e"] - bounds["w"], bounds["n"] - bounds["s"])


def get_cell_loc(p, cell_size):
    idx = int(math.floor(p / cell_size))
    return cell_size * idx


def line_to_geohashes(x0, y0, x1, y1, level):
    # effectively a ray tracing operation over a rectangular grid

    line_geohashes = set()

    cell_x_size, cell_y_size = geohash_cell_size(level)

    # compute the coordinate of the LL corner of each cell
    x0_cell_loc = get_cell_loc(x0, cell_x_size)
    y0_cell_loc = get_cell_loc(y0, cell_y_size)
    x1_cell_loc = get_cell_loc(x1, cell_x_size)
    y1_cell_loc = get_cell_loc(y1, cell_y_size)

    # compute the difference between the endpoints
    dx = math.fabs(x1 - x0)
    dy = math.fabs(y1 - y0)

    dt_dx = 1.0 / dx if dx != 0.0 else 0.0
    dt_dy = 1.0 / dy if dy != 0.0 else 0.0

    x = x0_cell_loc
    y = y0_cell_loc

    num_steps = 1
    x_inc = 0.0
    y_inc = 0.0
    t_next_horiz = 0.0
    t_next_vert = 0.0

    if dx == 0.0:
        x_inc = 0.0
        t_next_horiz = dt_dx * cell_x_size
    elif x1 > x0:
        x_inc = cell_x_size
        # number of horizontal intersections
        num_steps += (x1_cell_loc - x) / cell_x_size
        t_next_horiz = (x0_cell_loc + cell_x_size - x0) * dt_dx
    else:
        x_inc = -cell_x_size
        # number of horizontal intersections
        num_steps += (x - x1_cell_loc) / cell_x_size
        t_next_horiz = (x0 - x0_cell_loc) * dt_dx

    if dy == 0.0:
        y_inc = 0
        t_next_vert = dt_dy * cell_y_size
    elif y1 > y0:
        y_inc = cell_y_size
        # number of vertical intersections
        num_steps += (y1_cell_loc - y) / cell_y_size
        t_next_vert = (y0_cell_loc + cell_y_size - y0) * dt_dy
    else:
        y_inc = -cell_y_size
        # number of vertical intersections
        num_steps += (y - y1_cell_loc) / cell_y_size
        t_next_vert = (y0 - y0_cell_loc) * dt_dy

    for n in range(int(num_steps), 0, -1):
        line_geohashes.add(geohash.encode(y, x, level))

        if t_next_vert < t_next_horiz:
            y += y_inc
            t_next_vert += dt_dy * cell_y_size
        else:
            x += x_inc
            t_next_horiz += dt_dx * cell_x_size

    return line_geohashes


def trajectories_to_df(trajectories, level):
    trajectory_dataframes = []

    # loop over the trajectory dataframes and extract the flight paths as a list of x,y coords
    for tdf in trajectories:
        points = zip(tdf["x"], tdf["y"])
        last_p = next(points)
        geohashes = set()

        for p in points:
            geohashes = geohashes.union(
                line_to_geohashes(p[0], p[1], last_p[0], last_p[1], level)
            )
            last_p = p
        geohashes = list(geohashes)
        bounds = [helpers.geohash_to_array_str(gh) for gh in geohashes]

        gdf = pd.DataFrame(
            columns=["swarm_id", "altitude_id", "date", "geohash", "bounds"]
        )
        gdf["geohash"] = geohashes
        gdf["bounds"] = bounds
        gdf["swarm_id"] = tdf["swarm_id"]
        gdf["altitude_id"] = tdf["altitude_id"]
        gdf["date"] = tdf["date"]

        trajectory_dataframes.append(gdf)

    return pd.concat(trajectory_dataframes)


def debug():
    df = pd.read_csv("temp/swarm_7055_day3.txt", names=["id", "x", "y", "alt"])
    df = df.head(100)

    points = zip(df["x"], df["y"])
    last_p = next(points)
    geohashes = set()
    idx = 0
    for p in points:
        idx += 1
        if idx == 25:
            print("25")
        geohashes = geohashes.union(
            line_to_geohashes(p[0], p[1], last_p[0], last_p[1], 5)
        )
        last_p = p
    geohashes = list(geohashes)

    p = [geohash.decode(g) for g in geohashes]
    gh_p = list(zip(*p))
    pdf = pd.DataFrame(columns=["x", "y", "source"])
    pdf["x"] = gh_p[0]
    pdf["y"] = gh_p[1]
    pdf["source"] = False

    edf = pd.DataFrame(columns=pdf.columns)
    edf["y"] = df["x"]
    edf["x"] = df["y"]
    edf["source"] = True

    pdf = pd.concat([pdf, edf])

    xz, yz = geohash_cell_size(5)
    f1 = px.scatter(pdf, x="x", y="y", color="source")
    f1.update_yaxes(dtick=yz)
    f1.update_xaxes(dtick=xz)
    f1.show()


def main():
    args = parse_args()

    # debug()

    # copy and unzip hysplit zip files using the temp dir
    swarm_ids = extract_input(args.input_dir, args.temp_dir)

    # parse the control file to get the start date, altitudes and end time for each swarm
    swarm_control_info = parse_control_files(args.temp_dir, swarm_ids)

    # read in the day CSVs to genereate a timestamped set of swarm locations for each altitude track
    # ie. swarm_7055_1000m would be the location/time tuples for the 1000m prediction track
    trajectories = parse_trajectory_files(args.temp_dir, swarm_ids, swarm_control_info)

    # generate a final dataframe with all trajectories available
    trajectory_df = trajectories_to_df(trajectories, 5)
    trajectory_df.to_csv(args.output_file, index=False)

    # clean up
    shutil.rmtree(args.temp_dir)


if __name__ == "__main__":
    main()
