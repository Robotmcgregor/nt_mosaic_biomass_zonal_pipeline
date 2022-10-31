#!/usr/bin/env python

from __future__ import print_function, division
import fiona
import rasterio
import pandas as pd
from rasterstats import zonal_stats
import geopandas as gpd
import warnings
import os
from glob import glob
import numpy as np

warnings.filterwarnings("ignore")

'''
step1_9_seasonal_dja_zonal_stats.py
============================

Read in dja Landsat mosaics and extract zonal statistics for each 1ha plot.
Returns a csv file containing the statistics for each site for all files located within the specified directory.

Modified: Rob McGregor
email: robert.mcgregor@nt.gov.au
Date: 25/10/2022
version 1.0


###############################################################################################

MIT License

Copyright (c) 2022 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

========================================================================================================
'''

def landsat_correction_fn(output_zonal_stats, num_bands):
    """ Replace specific 0 values with Null values and correct b1, b2 and b3 calculations
    (refer to Fractional Cover metadata)

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats.
    @return: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated values.
    """

    for i in num_bands:

        output_zonal_stats['b{0}_dja_min'.format(str(i))] = output_zonal_stats['b{0}_dja_min'.format(str(i))].replace(0, np.nan)

        output_zonal_stats['b{0}_dja_min'.format(i)] = output_zonal_stats['b{0}_dja_min'.format(i)] - 100
        output_zonal_stats['b{0}_dja_max'.format(i)] = output_zonal_stats['b{0}_dja_max'.format(i)] - 100
        output_zonal_stats['b{0}_dja_mean'.format(i)] = output_zonal_stats['b{0}_dja_mean'.format(i)] - 100
        output_zonal_stats['b{0}_dja_med'.format(i)] = output_zonal_stats['b{0}_dja_med'.format(i)] - 100
        output_zonal_stats['b{0}_dja_p25'.format(i)] = output_zonal_stats['b{0}_dja_p25'.format(i)] - 100
        output_zonal_stats['b{0}_dja_p50'.format(i)] = output_zonal_stats['b{0}_dja_p50'.format(i)] - 100
        output_zonal_stats['b{0}_dja_p75'.format(i)] = output_zonal_stats['b{0}_dja_p75'.format(i)] - 100
        output_zonal_stats['b{0}_dja_p95'.format(i)] = output_zonal_stats['b{0}_dja_p95'.format(i)] - 100
        output_zonal_stats['b{0}_dja_p99'.format(i)] = output_zonal_stats['b{0}_dja_p99'.format(i)] - 100
        #output_zonal_stats['b{0}_dja_range'.format(i)] = output_zonal_stats['b{0}_dja_range'.format(i)] - 100


    return output_zonal_stats

def project_shapefile_gcs_wgs84_fn(albers, geo_df):
    """ Re-project a shapefile to 'GCSWGS84' to match the projection of the max_temp data.
    @param gcs_wgs84_dir: string object containing the path to the subdirectory located in the temporary_dir\gcs_wgs84
    @return:
    """

    # read in shp as a geoDataFrame.
    # df = gpd.read_file(zonal_stats_ready_dir + '\\' + complete_tile + '_by_tile.shp')

    # project to Australian Albers
    cgs_df = geo_df.to_crs(epsg=3577)

    # define crs file/path name variable.
    crs_name = 'albers'

    # Export re-projected shapefiles.
    projected_shape_path = albers + '\\' + 'geo_df_' + str(crs_name) + '.shp'

    # Export re-projected shapefiles.
    cgs_df.to_file(projected_shape_path)

    return cgs_df, projected_shape_path


def apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable, no_data):
    """
    Derive zonal stats for a list of Landsat imagery.

    @param image_s: string object containing the file path to the current max_temp tiff.
    @param projected_shape_path: string object containing the path to the current 1ha shapefile path.
    @param uid: ODK 1ha dataframe feature (unique numeric identifier)
    @return final_results: list object containing the specified zonal statistic values.
    """
    # create empty lists to write in  zonal stats results 

    zone_stats_list = []
    site_id_list = []
    image_name_list = []
    # print("Working on variable: ", variable)
    # print(qld_dict)
    # variable_values = qld_dict.get(variable)

    # print("variable_values: ", variable_values)
    no_data = no_data  # the no_data value for the silo max_temp raster imagery

    with rasterio.open(image_s, nodata=no_data) as srci:

        affine = srci.transform
        array = srci.read(1)

        #array = array - 100

        with fiona.open(projected_shape_path) as src:

            zs = zonal_stats(src, array, affine=affine, nodata=no_data,
                             stats=['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
                                    'percentile_75', 'percentile_95', 'percentile_99', 'range'], all_touched=True)

            # https://gis.stackexchange.com/questions/393413/rasterstats-zonal-statistics-does-not-ignore-nodata
            print(zs)
            # using "all_touched=True" will increase the number of pixels used to produce the stats "False" reduces
            # the number extract the image name from the opened file from the input file read in by rasterio


            path_, im_name = os.path.split(image_s)
            print("path_: ", path_)
            print("im_name: ", im_name)

            image_name_split = im_name.split("_")

            if str(image_name_split[-2]).startswith("m"):
                print("seasonal")
                im_date = image_name_split[-2]
            else:
                print("single date")
                im_date = image_name_split[-2]

            print("im_date: ", im_date)


            # list_a = str(srci).rsplit('\\')
            # # print("list_a: ", list_a)
            # file_name = list_a[-1]
            # # print("file_name: ", file_name)
            # list_b = file_name.rsplit("'")
            # file_name_final = list_b[0]
            # img_date = file_name_final[1:9]

            for zone in zs:
                zone_stats = zone
                count = zone_stats["count"]
                minimum = zone_stats["min"]
                maximum = zone_stats['max']
                mean = zone_stats["mean"]
                med = zone_stats['median']
                std = zone_stats['std']
                percentile_25 = zone_stats["percentile_25"]
                percentile_50 = zone_stats["percentile_50"]
                percentile_75 = zone_stats['percentile_75']
                percentile_95 = zone_stats['percentile_95']
                percentile_99 = zone_stats['percentile_99']
                range_ = zone_stats['range']

                # put the individual results in a list and append them to the zone_stats list
                result = [minimum, maximum, mean, count,  std, med, range_, percentile_25, percentile_50,
                          percentile_75, percentile_95, percentile_99, ]



                zone_stats_list.append(result)

            # extract out the site number for the polygon
            for i in src:
                table_attributes = i['properties']  # reads in the attribute table for each record

                ident = table_attributes[
                    uid]  # reads in the id field from the attribute table and prints out the selected record
                site = table_attributes['site_name']

                details = [ident, site, im_date]

                site_id_list.append(details)
                image_used = [im_name]
                image_name_list.append(image_used)

        # join the elements in each of the lists row by row
        final_results = [siteid + zoneR + imU for siteid, zoneR, imU in
                         zip(site_id_list, zone_stats_list, image_name_list)]

        # close the vector and raster file 
        src.close()
        srci.close()

    return final_results



def time_stamp_fn(output_zonal_stats):
    """Insert a timestamp into feature position 4, convert timestamp into year, month and day strings and append to
    dataframe.

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats
    @return output_zonal_stats: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated features.
    """

    s_year_ = []
    s_month_ = []
    s_day_ = []
    s_date_ = []
    e_year_ = []
    e_month_ = []
    e_day_ = []
    e_date_ = []

    import calendar
    print("init time stamp")
    # Convert the date to a time stamp
    for n in output_zonal_stats.date:
        print("n: ", n)
        i = n[1:]
        print(i)
        s_year = i[:4]
        s_month = i[4:6]
        s_day = "01"
        s_date = str(s_year) + str(s_month) + str(s_day)

        s_year_.append(s_year)
        s_month_.append(s_month)
        s_day_.append(s_day)
        s_date_.append(s_date)

        e_year = i[6:10]
        e_month = i[10:12]
        m, d = calendar.monthrange(int(e_year), int(e_month))
        e_day = str(d)
        if len(e_day) < 1:
            d_ = "0" + str(d)
        else:
            d_ = str(d)

        e_date = str(e_year) + str(e_month) + str(d_)

        e_year_.append(e_year)
        e_month_.append(e_month)
        e_day_.append(e_day)
        e_date_.append(e_date)

    output_zonal_stats.insert(4, 'e_date', e_date_)
    output_zonal_stats.insert(4, 'e_year', e_year_)
    output_zonal_stats.insert(4, 'e_month', e_month_)
    output_zonal_stats.insert(4, 'e_day', e_day_)

    output_zonal_stats.insert(4, 's_date', s_date_)
    output_zonal_stats.insert(4, 's_year', s_year_)
    output_zonal_stats.insert(4, 's_month', s_month_)
    output_zonal_stats.insert(4, 's_day', s_day_)

    pd.to_datetime(output_zonal_stats.s_date, format='%Y%m%d')

    pd.to_datetime(output_zonal_stats.e_date, format='%Y%m%d')

    return output_zonal_stats


def clean_data_frame_fn(output_list, output_dir, var_, band):
    """ Create dataframe from output list, clean and export dataframe to a csv to export directory/max_temp sub-directory.

    @param output_list: list object created by appending the final results list elements.
    @param max_temp_output_dir: string object containing the path to the export directory/max_temp sub-directory .
    @param complete_tile: string object containing the current Landsat tile information.
    @return output_max_temp: dataframe object containing all max_temp zonal stats based on the ODK 1ha plots created
    based on the current Landsat tile.
    """

    headers = ["uid",
               'site',
               'date',
               "b" + str(band) + '_dja_min',
              "b" + str(band) + '_dja_max',
               "b" + str(band) + '_dja_mean',
               "b" + str(band) + '_dja_count',
              "b" + str(band) + '_dja_std',
               "b" + str(band) + '_dja_med',
               "b" + str(band) + '_dja_range',
              "b" + str(band) + '_dja_p25',
               "b" + str(band) + '_dja_p50',
               "b" + str(band) + '_dja_p75',
              "b" + str(band) + '_dja_p95',
               "b" + str(band) + '_dja_p99',
               'image']


    output = pd.DataFrame.from_records(output_list)
    output.to_csv(r"Z:\Scratch\Rob\test.csv")
    for i in output.columns:
        print(i)

    print(output)

    output = pd.DataFrame.from_records(output_list, columns=headers)
    # print('output_max_temp: ', output_max_temp)

    print("output: ", output.columns)
    # Convert the date to a time stamp
    output = time_stamp_fn(output)

    # remove 100 from zone_stats
    output = landsat_correction_fn(output, [band])

    print("output2: ", output.columns)
    output = output[['uid', 'site', 'image', 's_day', 's_month', 's_year', 's_date', 'e_day', 'e_month', 'e_year',
                     'e_date', 'b1_dja_count', 'b1_dja_min', 'b1_dja_max',
                     'b1_dja_mean', 'b1_dja_med', 'b1_dja_std', 'b1_dja_p25', 'b1_dja_p50', 'b1_dja_p75',
                     'b1_dja_p95','b1_dja_p99', 'b1_dja_range']]

    site = output['site'].unique()

    print("length of site list: ", len(site))
    if len(site) >= 1:
        for i in site:
            out_df = output[output['site'] == i]

            out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
                str(i), var_))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
            str(site), var_))
        # export the pandas df to a csv file
        output.to_csv(out_path, index=False)

    return output


def main_routine(export_dir_path, variable, csv_file, temp_dir_path, geo_df, no_data):
    """ Calculate the zonal statistics for each 1ha site per QLD monthly max_temp image (single band).
    Concatenate and clean final output DataFrame and export to the Export directory/zonal stats.

    export_dir_path, zonal_stats_ready_dir, fpc_output_zonal_stats, fpc_complete_tile, i, csv_file, temp_dir_path, qld_dict"""

    print("Mosaic DJA zonal stats beginning.........")
    print("no_data: ", no_data, " - should be 0")

    uid = 'uid'
    output_list = []
    print("variable: ", variable)

    band = 1

    albers_dir = os.path.join(temp_dir_path, "albers")

    # # define the GCSWGS84 directory pathway
    # gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    #
    # define the max_tempOutput directory pathway
    output_dir = (os.path.join(export_dir_path, "{0}_zonal_stats".format(variable)))

    # call the project_shapefile_gcs_wgs84_fn function
    cgs_df, projected_shape_path = project_shapefile_gcs_wgs84_fn(albers_dir, geo_df)

    # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
    with open(csv_file, 'r') as imagery_list:

        # loop through the list of imagery and input the image into the raster zonal_stats function
        for image in imagery_list:
            # print('image: ', image)

            image_s = image.rstrip()
            print("image_s: ", image_s)

            final_results = apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable,
                                                 no_data)  # cgs_df,projected_shape_path,

            for i in final_results:
                output_list.append(i)

    # call the clean_data_frame_fn function
    clean_output_temp = clean_data_frame_fn(output_list, output_dir, variable, band)






    return projected_shape_path


if __name__ == "__main__":
    main_routine()
