"""module used to create a matrix containing the distances between the postcodes that specify the locations of the
 specified geographies"""

import os
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import csv
import datetime as dt


def calc_dist(inlat1, inlat2, inlong1, inlong2):
    """simple function that returns a distance between points given lat and longs of points.
    Takes curvature of earth into account"""
    R = 6373.0

    lat1 = radians(inlat1)
    lon1 = radians(inlong1)
    lat2 = radians(inlat2)
    lon2 = radians(inlong2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def produce_distance_matrix(input_data, output_name, geog, lat, long):
    """takes input data, output path including filename with .csv and the columns containing the required data.
    In this case the reference to the geography and the lat and long"""

    start_time = dt.datetime.now()
    print('Started at: ', start_time)

    fields = [geog, lat, long]
    info_df = pd.read_csv(input_data, usecols=fields)

    geog_list = fields[0]
    lat_list = fields[1]
    long_list = fields[2]

    name_list = list(info_df[geog_list])
    lat_list = list(info_df[lat_list])
    long_list = list(info_df[long_list])

    # create csv with headers as names
    with open(output_name, 'w') as myfile:
        wr = csv.writer(myfile)
        wr.writerow([geog] + name_list)

    # for each postcode in the file
    for i in range(0, len(info_df)):

        current = name_list[i]
        current_lat = lat_list[i]
        current_long = long_list[i]

        temp_dist = []  # list of distance results

        # calculate distances to all postcodes in file
        for j in range(0, len(info_df)):
            temp_lat = lat_list[j]
            temp_long = long_list[j]

            dist = calc_dist(current_lat, temp_lat, current_long,  temp_long)

            # create lists of postcodes and distances
            temp_dist.append(dist)

        if i > 0 and i % 1000 == 0:
            time_now = dt.datetime.now()
            time_left = ((time_now - start_time).seconds/(i/len(name_list))) - (time_now - start_time).seconds
            finish_time = time_now + dt.timedelta(seconds=time_left)

            print('Row ', i, 'reached. Projected finish time is: ', finish_time)

        temp_dist = [current] + temp_dist

        with open(output_name, 'a') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(temp_dist)


# location of input data
os.chdir(os.path.join(os.getcwd(), 'raw_inputs'))
# input data to include
produce_distance_matrix('LSOA_L_L.csv', 'lsoa_matrix_full.csv', 'lsoa11cd', 'lat', 'long')


