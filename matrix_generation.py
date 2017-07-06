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
    """ produces a matrix of distance bwtween lat and logs for given level of geography. e.g:

       A B C
     A 0 2 4
     B 2 0 5
     C 4 5 0

     Where A, B and C represent the codes of the geographies.

    Requires:

    input_data - csv file of lat and longs of each item (e.g. lsoa, la, postcode etc)
    output_name - output filename
    geog - the name of the column with the codes
    lat - name of the latitude column
    long - name of the longitude column
    """

    start_time = dt.datetime.now()
    print('Started at: ', start_time)

    # load data
    fields = [geog, lat, long]
    info_df = pd.read_csv(input_data, usecols=fields)

    # create lists from dataframe - faster to index
    name_list = list(info_df[geog])
    lat_list = list(info_df[lat])
    long_list = list(info_df[long])

    # create output csv with headers as names
    with open(output_name, 'w') as myfile:
        wr = csv.writer(myfile)
        wr.writerow([geog] + name_list)

    # for each record in the input
    for i in range(0, len(info_df)):

        current = name_list[i]
        current_lat = lat_list[i]
        current_long = long_list[i]

        temp_dist = []  # list to store  distance results

        # calculate distances to all other records in input
        for j in range(0, len(info_df)):
            temp_lat = lat_list[j]
            temp_long = long_list[j]

            dist = calc_dist(current_lat, temp_lat, current_long,  temp_long)
            # investigate if putting zero distances to NaN makes finding min easier...

            # create lists of distances
            temp_dist.append(dist)

        # report on progress based on current records processed
        if i > 0 and i % 1000 == 0:
            time_now = dt.datetime.now()
            time_left = ((time_now - start_time).seconds/(i/len(name_list))) - (time_now - start_time).seconds
            finish_time = time_now + dt.timedelta(seconds=time_left)

            print('Row ', i, 'reached. Projected finish time is: ', finish_time)

        temp_dist = [current] + temp_dist

        # write to output file - append
        with open(output_name, 'a') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(temp_dist)


# location of input data
os.chdir(os.path.join(os.getcwd(), 'raw_inputs'))
# input data to include
produce_distance_matrix('LSOA_L_L.csv', 'lsoa_matrix_full.csv', 'lsoa11cd', 'lat', 'long')


