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
import shutil

warnings.filterwarnings("ignore")

'''
step1_7_monthly_max_temp_zonal_stats.py
============================

Read in max_temp raster images from QLD silo and a polygon shapefile and perform zonal statistic analysis on a list of 
imagery. It returns a csv file containing the statistics for the input zones.

Author: Grant Staben
email: grant.staben@nt.gov.au
Date: 21/09/2020
version: 1.0

Modified: Rob McGregor
email: robert.mcgregor@nt.gov.au
Date: 2/11/2020
version 2.0


###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

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


def apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable, no_data, band):
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

    # create empty lists to append values
    zone_stats = []
    list_site = []
    list_uid = []
    list_prop = []
    list_prop_code = []
    list_site_date = []
    list_image_name = []
    image_date = []
    list_band = []


    with rasterio.open(image_s, nodata=no_data) as srci:

        affine = srci.transform
        array = srci.read(band)

        #array = array - 100

        with fiona.open(projected_shape_path) as src:

            zs = zonal_stats(src, array, affine=affine, nodata=-no_data,
                             stats=['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
                                    'percentile_75', 'percentile_95', 'percentile_99', 'range'], all_touched=True)

            print(zs)
            # using "all_touched=True" will increase the number of pixels used to produce the stats "False" reduces
            # the number extract the image name from the opened file from the input file read in by rasterio

            path_, im_name = os.path.split(image_s)
            print("path_: ", path_)
            print("im_name: ", im_name)

            image_name_split = im_name.split("_")

            if str(image_name_split[-2]).startswith("m"):
                print("seasonal")
                im_date = image_name_split[-2][1:]
            else:
                print("single date")
                im_date = image_name_split[-2]

            print("im_date: ", im_date)

            for zone in zs:

                bands = 'b' + str(band)
                list_band.append(bands)

                # extract 'values' as a tuple from a dictionary
                keys, values = zip(*zone.items())
                # convert tuple to a list and append to zone_stats
                result = list(values)
                print("Results: ", result)
                zone_stats.append(result)

            for i in src:
                # extract shapefile records
                table_attributes = i['properties']

                uid_ = table_attributes[uid]
                details = [uid_]
                list_uid.append(details)

                site = table_attributes['site_name']
                site_ = [site]
                print("site_: ", site)
                list_site.append(site_)

                # join the elements in each of the lists row by row
            final_results = [list_uid + list_site + zone_stats for
                             list_uid, list_site, zone_stats in
                             zip(list_uid, list_site, zone_stats)]

            print("final_results: ", final_results)
            # close the vector and raster file
            src.close()
            srci.close()

        print("final results:", final_results)
        print("list_site: ", list_site)
        print("str(site_[0]): ", str(site_[0]))
        return final_results, str(site_[0])




    #             zone_stats = zone
    #             count = zone_stats["count"]
    #             mean = zone_stats["mean"]
    #             minimum = zone_stats["min"]
    #             maximum = zone_stats['max']
    #             med = zone_stats['median']
    #             std = zone_stats['std']
    #             percentile_25 = zone_stats["percentile_25"]
    #             percentile_50 = zone_stats["percentile_50"]
    #             percentile_75 = zone_stats['percentile_75']
    #             percentile_95 = zone_stats['percentile_95']
    #             percentile_99 = zone_stats['percentile_99']
    #             range_ = zone_stats['range']
    #
    #             # put the individual results in a list and append them to the zone_stats list
    #             result = [mean, std, med, minimum, maximum, count, percentile_25, percentile_50,
    #                       percentile_75, percentile_95, percentile_99, range_]
    #             zone_stats_list.append(result)
    #
    #         # extract out the site number for the polygon
    #         for i in src:
    #             table_attributes = i['properties']  # reads in the attribute table for each record
    #
    #             ident = table_attributes[
    #                 uid]  # reads in the id field from the attribute table and prints out the selected record
    #             site = table_attributes['site_name']
    #
    #             details = [ident, site, im_date]
    #
    #             site_id_list.append(details)
    #             image_used = [im_name]
    #             image_name_list.append(image_used)
    #
    #     # join the elements in each of the lists row by row
    #     final_results = [siteid + zoneR + imU for siteid, zoneR, imU in
    #                      zip(site_id_list, zone_stats_list, image_name_list)]
    #
    #     # close the vector and raster file
    #     src.close()
    #     srci.close()
    #
    # return final_results


def clean_data_frame_fn(output_list, output_dir, var_):
    """ Create dataframe from output list, clean and export dataframe to a csv to export directory/max_temp sub-directory.

    @param output_list: list object created by appending the final results list elements.
    @param max_temp_output_dir: string object containing the path to the export directory/max_temp sub-directory .
    @param complete_tile: string object containing the current Landsat tile information.
    @return output_max_temp: dataframe object containing all max_temp zonal stats based on the ODK 1ha plots created
    based on the current Landsat tile.
    """

    # convert the list to a pandas dataframe with a headers
    headers = ['ident', 'site', 'im_date', var_ + '_mean', var_ + '_std', var_ + '_med', var_ + '_min',
               var_ + '_max', var_ + '_count', var_ + "_p25", var_ + "_p50", var_ + "_p75", var_ + "_p95",
               var_ + "_p99", var_ + "_rng", 'im_name']

    output_max_temp = pd.DataFrame.from_records(output_list, columns=headers)
    # print('output_max_temp: ', output_max_temp)

    site = output_max_temp['site'].unique()

    print("length of site list: ", len(site))
    if len(site) >= 1:
        for i in site:
            out_df = output_max_temp[output_max_temp['site'] == i]

            out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
                str(i), var_))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
            str(site), var_))
        # export the pandas df to a csv file
        output_max_temp.to_csv(out_path, index=False)

    return output_max_temp


def main_routine(export_dir_path, variable, csv_file, temp_dir_path, geo_df, no_data):
    """ Calculate the zonal statistics for each 1ha site per QLD monthly max_temp image (single band).
    Concatenate and clean final output DataFrame and export to the Export directory/zonal stats.

    export_dir_path, zonal_stats_ready_dir, fpc_output_zonal_stats, fpc_complete_tile, i, csv_file, temp_dir_path, qld_dict"""

    print("Mosaic DIM zonal stats beginning.........")
    print("no_data: ", no_data, " - should be 0")

    uid = 'uid'
    output_list = []
    print("variable: ", variable)

    albers_dir = os.path.join(temp_dir_path, "albers")
    print("albers Dir: ", albers_dir)

    # # define the GCSWGS84 directory pathway
    # gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    #
    # define the max_tempOutput directory pathway
    output_dir = (os.path.join(export_dir_path, "{0}_zonal_stats".format(variable)))

    # call the project_shapefile_gcs_wgs84_fn function
    cgs_df, projected_shape_path = project_shapefile_gcs_wgs84_fn(albers_dir, geo_df)

    # create temporary folders
    dim_temp_dir_bands = os.path.join(temp_dir_path, 'dim_temp_individual_bands')
    os.makedirs(dim_temp_dir_bands)
    # band1_dir = os.path.join(dim_temp_dir_bands, 'band1')
    # os.makedirs(band1_dir)
    # band2_dir = os.path.join(dim_temp_dir_bands, 'band2')
    # os.makedirs(band2_dir)
    # band3_dir = os.path.join(dim_temp_dir_bands, 'band3')
    # os.makedirs(band3_dir)
    num_bands = [1, 2, 3, 4, 5, 6]

    for i in num_bands:
        band_dir = os.path.join(dim_temp_dir_bands, 'band{0}'.format(str(i)))
        os.makedirs(band_dir)


    # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
    with open(csv_file, 'r') as imagery_list:

        for band in num_bands:

            # loop through the list of imagery and input the image into the raster zonal_stats function
            for image in imagery_list:
                # print('image: ', image)

                image_s = image.rstrip()
                print("image_s: ", image_s)
                path_, im_name = os.path.split(image_s)

                # print("im_name_s: ", im_name_s)
                print('Image name: ', im_name)

                image_name_split = im_name.split("_")

                if str(image_name_split[-2]).startswith("m"):
                    print("seasonal")
                    im_date = image_name_split[-2][1:]
                else:
                    print("single date")
                    im_date = image_name_split[-2]

                # loops through each image
                with rasterio.open(image_s, nodata=no_data) as srci:
                    image_results = 'image_' + im_name + '.csv'

                    final_results, site = apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable,
                                                         no_data, band)


                    header = ["b" + str(band) + '_uid', "b" + str(band) + '_site', "b" + str(band) + '_count',
                              "b" + str(band) + '_min', "b" + str(band) + '_max',
                              "b" + str(band) + '_mean', "b" + str(band) + '_median', "b" + str(band) + '_std',
                              "b" + str(band) + '_p25', "b" + str(band) + '_p50', "b" + str(band) + '_p75',
                              "b" + str(band) + '_p95', "b" + str(band) + '_p99', "b" + str(band) + '_range']

                    df = pd.DataFrame.from_records(final_results, columns=header)

                    df['band'] = band
                    df['image'] = im_name
                    df['date'] = im_date
                    df.to_csv(dim_temp_dir_bands + '//band' + str(band) + '//' + image_results, index=False)




                # final_results = apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable,
                #                                      no_data)  # cgs_df,projected_shape_path,
                #
                # for i in final_results:
                #     output_list.append(i)

    # # call the clean_data_frame_fn function
    # clean_output_temp = clean_data_frame_fn(output_list, output_dir, variable)
    # for loops through the band folders and concatenates zonal stat outputs into a complete band specific csv

    for x in num_bands:
        location_output = dim_temp_dir_bands + '//band' + str(x)
        band_files = glob.glob(os.path.join(location_output,
                                            '*.csv'))

        # advisable to use os.path.join as this makes concatenation OS independent
        df_from_each_band_file = (pd.read_csv(f) for f in band_files)
        concat_band_df = pd.concat(df_from_each_band_file, ignore_index=False, axis=0, sort=False)
        # export the band specific results to a csv file (i.e. three outputs)
        print("output csv to: ", dim_temp_dir_bands + '//' + 'Band' + str(x) + '_test.csv')
        concat_band_df.to_csv(dim_temp_dir_bands + '//' + 'Band' + str(x) + '_test.csv', index=False)

    # ----------------------------------------- Concatenate three bands together ---------------------------------------

    # Concatenate Three bands
    header_all = ['uid', 'site', 'b1_dim_count', 'b1_dim_min', 'b1_dim_max', 'b1_dim_mean',
                  'b1_dim_med', 'b1_dim_std', 'b1_dim_p25', 'b1_dim_p50', 'b1_dim_p75', 'b1_dim_p95', 'b1_dim_p99',
                  'b1_dim_range', 'band', 'dim_image', 'date',

                  'b2_uid', 'b2_site', 'b2_dim_count', 'b2_dim_min', 'b2_dim_max', 'b2_dim_mean',
                  'b2_dim_med', 'b2_dim_std', 'b2_dim_p25', 'b2_dim_p50', 'b2_dim_p75', 'b2_dim_p95', 'b2_dim_p99',
                  'b2_dim_range', 'b2_dim_band', 'b2_dim_im', 'b2_dim_date',

                  'b3_uid', 'b3_site', 'b3_dim_count', 'b3_dim_min', 'b3_dim_max', 'b3_dim_mean',
                  'b3_dim_med', 'b3_dim_std', 'b3_dim_p25', 'b3_dim_p50', 'b3_dim_p75', 'b3_dim_p95', 'b3_dim_p99',
                  'b3_dim_range', 'b3_dim_band', 'b3_dim_im', 'b3_dim_date',

                  'b4_uid', 'b4_site', 'b4_dim_count', 'b4_dim_min', 'b4_dim_max', 'b4_dim_mean',
                  'b4_dim_med', 'b4_dim_std', 'b4_dim_p25', 'b4_dim_p50', 'b4_dim_p75', 'b4_dim_p95', 'b4_dim_p99',
                  'b4_dim_range', 'b4_dim_band', 'b4_dim_im', 'b4_dim_date',

                  'b5_uid', 'b5_site', 'b5_dim_count', 'b5_dim_min', 'b5_dim_max', 'b5_dim_mean',
                  'b5_dim_med', 'b5_dim_std', 'b5_dim_p25', 'b5_dim_p50', 'b5_dim_p75', 'b5_dim_p95', 'b5_dim_p99',
                  'b5_dim_range', 'b5_dim_band', 'b5_dim_im', 'b5_dim_date',

                  'b6_uid', 'b6_site', 'b6_dim_count', 'b6_dim_min', 'b6_dim_max', 'b6_dim_mean',
                  'b6_dim_med', 'b6_dim_std', 'b6_dim_p25', 'b6_dim_p50', 'b6_dim_p75', 'b6_dim_p95', 'b6_dim_p99',
                  'b6_dim_range', 'b6_dim_band', 'b6_dim_im', 'b6_dim_date']

    all_files = glob.glob(os.path.join(dim_temp_dir_bands,
                                       '*.csv'))
    # advisable to use os.path.join as this makes concatenation OS independent
    df_from_each_file = (pd.read_csv(f) for f in all_files)
    output_zonal_stats = pd.concat(df_from_each_file, ignore_index=False, axis=1, sort=False)
    print("-" * 50)
    print(output_zonal_stats.shape)
    print(output_zonal_stats.columns)

    output_zonal_stats.columns = header_all

    # -------------------------------------------------- Clean dataframe -----------------------------------------------
    # output_zonal_stats.to_csv(r"Z:\Scratch\Rob\output_zonal_stats2.csv")
    # Convert the date to a time stamp
    # time_stamp_fn(output_zonal_stats)

    # remove 100 from zone_stats
    # landsat_correction_fn(output_zonal_stats)

    # reshape the final dataframe
    output_zonal_stats = output_zonal_stats[
        ['uid', 'site', 'dim_image', 'year', 'month', 'day', 'b1_dim_count', 'b1_dim_min',
         'b1_dim_max', 'b1_dim_mean', 'b1_dim_med', 'b1_dim_std',
         'b1_dim_p25', 'b1_dim_p50', 'b1_dim_p75', 'b1_dim_p95', 'b1_dim_p99', 'b1_dim_range',
         'b2_dim_count', 'b2_dim_min', 'b2_dim_max', 'b2_dim_mean', 'b2_dim_med', 'b2_dim_std',
         'b2_dim_p25', 'b2_dim_p50', 'b2_dim_p75', 'b2_dim_p95', 'b2_dim_p99',
         'b2_dim_range', 'b3_dim_count', 'b3_dim_min', 'b3_dim_max', 'b3_dim_mean', 'b3_dim_med',
         'b3_dim_std', 'b3_dim_p25', 'b3_dim_p50', 'b3_dim_p75', 'b3_dim_p95', 'b3_dim_p99', 'b3_dim_range',

         'b4_dim_count', 'b4_dim_min', 'b4_dim_max', 'b4_dim_mean', 'b4_dim_med',
         'b4_dim_std', 'b4_dim_p25', 'b4_dim_p50', 'b4_dim_p75', 'b4_dim_p95', 'b4_dim_p99', 'b4_dim_range',

         'b5_dim_count', 'b5_dim_min', 'b5_dim_max', 'b5_dim_mean', 'b5_dim_med',
         'b5_dim_std', 'b5_dim_p25', 'b5_dim_p50', 'b5_dim_p75', 'b5_dim_p95', 'b5_dim_p99', 'b5_dim_range',

         'b6_dim_count', 'b6_dim_min', 'b6_dim_max', 'b6_dim_mean', 'b6_dim_med',
         'b6_dim_std', 'b6_dim_p25', 'b6_dim_p50', 'b6_dim_p75', 'b6_dim_p95', 'b6_dim_p99', 'b6_dim_range',
         ]]

    site_list = output_zonal_stats.site.unique().tolist()
    print("length of site list: ", len(site_list))
    if len(site_list) >= 1:
        for i in site_list:
            out_df = output_zonal_stats[output_zonal_stats['site'] == i]

            out_path = os.path.join(export_dir_path, "{0}_dim_zonal_stats.csv".format(str(i)))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(export_dir_path,
                                "{0}_dim_zonal_stats.csv".format(str(site_list[0])))
        # export the pandas df to a csv file
        output_zonal_stats.to_csv(out_path, index=False)

    # ----------------------------------------------- Delete temporary files -------------------------------------------
    # remove the temp dir and single band csv files
    shutil.rmtree(dim_temp_dir_bands)

    print('=' * 50)

    return projected_shape_path


if __name__ == "__main__":
    main_routine()
