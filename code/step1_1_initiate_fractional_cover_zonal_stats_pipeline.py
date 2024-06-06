#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description: This pipeline comprises 11 scripts which read in the AGB biomass csv. Pipeline converts data to
geo-dataframe and created a 1ha polygon (site) for each point. Once this is complete the pipeline runs zonal statistics
on the current Landsat mosaic and exports a csv per site into an outputs directory. Note, seasonal composites can be
6 band, 3 band, greyscale and classified.
Once pipeline is complete a temporary directory which was created will be deleted from the working drive, if script fails
the temporary directory requires manual deletion.


step1_1_initiate_fractional_cover_zonal_stats_pipeline.py
===============================
Description: This script initiates the NT Mosaic biomass zonal pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. deletes the temporary directory and its contents once the pipeline has completed.


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 30/10/2022
Version: 1.0

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

===================================================================================================

Command arguments:
------------------

 - tile_grid
    - String object containing the path to the Landsat tile grid shapefile.

 - data:
    - String object containing the file path to the agb biomass csv created from biomass_field_data_clean_v4.ipynb notebook.
Refer to previous section for csv feature requirements.

 - export_dir:
    - String object containing the path to a directory. An export directory tree will be created here, with all outputs
exported here.

 - mosaic_dir
    - String object containing the path to the Landsat seasonal mosaic directory (default value is r'Z:\Landsat\mosaic').
   Note: deviation from this structure fill cause the pipeline to fail; however, path changes can be easily made on
   step1_1_initiate_fractional_cover_zonal_stats_pipeline.py


======================================================================================================

"""

# Import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings

warnings.filterwarnings("ignore")


def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')


    p.add_argument('-d', '--data', help='The directory the site points csv file.')

    p.add_argument('-x', '--export_dir',
                   help='Enter the export directory for all of the final outputs.',
                   default=r'U:\scratch\rob\pipelines\outputs')

    # p.add_argument('-i', '--image_count', type=int,
    #                help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
    #                default=5)

    p.add_argument('-l', '--mosaics_dir', help="The NT seasonal mosaics directory path",
                   default=r"R:\landsat\mosaics")

    # p.add_argument('-n', '--no_data', help="Enter the Landsat Fractional Cover no data value (i.e. 0)",
    #                default=0)


    cmd_args = p.parse_args()

    if cmd_args.data is None:
        p.print_help()

        sys.exit()

    return cmd_args


def temporary_dir_fn():
    """ Create a temporary directory 'user_YYYMMDD_HHMM'.

    @return temp_dir_path: string object containing the newly created directory path.
    @return final_user: string object containing the user id or the operator.
    """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir_path = '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(temp_dir_path)

    except:
        print('The following temporary directory will be created: ', temp_dir_path)
        pass
    # create folder a temporary folder titled (titled 'tempFolder'
    os.makedirs(temp_dir_path)

    return temp_dir_path, final_user


def temp_dir_folders_fn(temp_dir_path):
    """ Create folders within the temp_dir directory.

    @param temp_dir_path: string object containing the newly created directory path.
    @return prime_temp_grid_dir: string object containing the newly created folder (temp_tile_grid) within the
    temporary directory.
    @return prime_temp_buffer_dir: string object containing the newly created folder (temp_1ha_buffer)within the
    temporary directory.

    """

    prime_temp_grid_dir = temp_dir_path + '\\temp_tile_grid'
    os.mkdir(prime_temp_grid_dir)

    zonal_stats_ready_dir = prime_temp_grid_dir + '\\zonal_stats_ready'
    os.makedirs(zonal_stats_ready_dir)

    proj_tile_grid_sep_dir = prime_temp_grid_dir + '\\separation'
    os.makedirs(proj_tile_grid_sep_dir)

    prime_temp_buffer_dir = temp_dir_path + '\\temp_1ha_buffer'
    os.mkdir(prime_temp_buffer_dir)

    gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    os.mkdir(gcs_wgs84_dir)

    albers_dir = (temp_dir_path + '\\albers')
    os.mkdir(albers_dir)

    return prime_temp_grid_dir, prime_temp_buffer_dir, zonal_stats_ready_dir


def export_file_path_fn(export_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument export_dir.

    @param final_user: string object containing the user id or the operator.
    @param export_dir: string object containing the path to the export directory (command argument).
    @return export_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create string object from final_user and datetime.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = export_dir + '\\' + final_user + '_nt_mosaic_' + str(
        date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        print('The following export directory will be created: ', export_dir_path)
        pass

    # create folder.
    os.makedirs(export_dir_path)

    return export_dir_path


def export_dir_folders_fn(export_dir_path):
    """ Create sub-folders within the export directory.

    @param export_dir_path: string object containing the newly created export directory path.
    @return tile_status_dir: string object containing the newly created folder (tile_status) with three sub-folders:
    for_processing, insufficient_files and tile_status_lists.
    @return tile_status_dir:
    @return plot_dir:
    @return zonal_stats_output_dir:
    @return rainfall_output_dir:
    """

    h99a2_zonal_stats_output_dir = (export_dir_path + '\\h99a2_zonal_stats')
    print("h99a2_zonal_stats_output_dir: ", h99a2_zonal_stats_output_dir)
    os.mkdir(h99a2_zonal_stats_output_dir)

    fpca2_zonal_stats_output_dir = (export_dir_path + '\\fpca2_zonal_stats')
    os.mkdir(fpca2_zonal_stats_output_dir)
    #
    dp0_zonal_stats_output_dir = (export_dir_path + '\\dp0_zonal_stats')
    os.mkdir(dp0_zonal_stats_output_dir)

    dp1_zonal_stats_output_dir = (export_dir_path + '\\dp1_zonal_stats')
    os.mkdir(dp1_zonal_stats_output_dir)
    #
    dbi_zonal_stats_output_dir = (export_dir_path + '\\dbi_zonal_stats')
    os.mkdir(dbi_zonal_stats_output_dir)

    dim_zonal_stats_output_dir = (export_dir_path + '\\dim_zonal_stats')
    os.mkdir(dim_zonal_stats_output_dir)

    dis_zonal_stats_output_dir = (export_dir_path + '\\dis_zonal_stats')
    os.mkdir(dis_zonal_stats_output_dir)

    dja_zonal_stats_output_dir = (export_dir_path + '\\dja_zonal_stats')
    os.mkdir(dja_zonal_stats_output_dir)

    dka_zonal_stats_output_dir = (export_dir_path + '\\dka_zonal_stats')
    os.mkdir(dka_zonal_stats_output_dir)

    stc_zonal_stats_output_dir = (export_dir_path + '\\stc_zonal_stats')
    os.mkdir(stc_zonal_stats_output_dir)
    #



def main_routine():
    """" Description: This pipeline creates a 1ha plot from biomass extent point data and extracts zonal statistics
    from the following Landsat mosaics:
     - h99a2
     - fpca2
     - dbi
     - dim
     - dis
     - dja
     - dka
     - stc -d "U:\biomass\collated_agb\20230927\slats_tern_biomass.csv"

    Output 1: Zonal statistic csv for each sites per file type if situated with mosaic boundary.
    Output 2: GDA94 point shapefile
    Output 2: GDA94 1ha polygon shapefile
    Output 2: Australian Albers 1ha polygon 1ha shapefile."""

    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    data = cmd_args.data
    export_dir = cmd_args.export_dir
    mosaics_dir = cmd_args.mosaics_dir


    # call the temporaryDir function.
    temp_dir_path, final_user = temporary_dir_fn()
    # call the tempDirFolders function.
    prime_temp_grid_dir, prime_temp_buffer_dir, zonal_stats_ready_dir = temp_dir_folders_fn(temp_dir_path)
    # call the exportFilepath function.
    export_dir_path = export_file_path_fn(export_dir, final_user)

    export_dir_folders_fn(export_dir_path)

    print(data)
    import step1_3_project_buffer
    geo_df2, crs_name = step1_3_project_buffer.main_routine(data, export_dir_path, prime_temp_buffer_dir)

    geo_df2.reset_index(drop=True, inplace=True)
    geo_df2['uid'] = geo_df2.index + 1

    shapefile_path = os.path.join(export_dir_path, "biomass_1ha_all_sites.shp")
    geo_df2.to_file(os.path.join(shapefile_path),
                    driver="ESRI Shapefile")

    print("Exported shapefile: ", shapefile_path)
    # ----------------------------------------------------- h99 --------------------------------------------------------

    print("-" * 50)
    print("Creating lists of tiff's")
    print("-" * 50)

    h99a2_dir = os.path.join(mosaics_dir, "structural_formation", "h99_mos")
    print("h99: ")
    import step1_2_list_of_images
    h99a2_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, h99a2_dir, 'h99a2', "*h99a2*.img")

    # ---------------------------------------------------- fpca2 -------------------------------------------------------

    fpca2_dir = os.path.join(mosaics_dir, "structural_formation", "h99_mos")
    print("Fpc: ")
    fpca2_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, fpca2_dir, 'fpca2', "*fpca2*.img")

    # ---------------------------------------------------- dbi ---------------------------------------------------------

    dbi_dir = os.path.join(mosaics_dir, "SeasonalComposites", "dbi")
    print("Dbi: ")
    dbi_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, dbi_dir, 'dbi', "*dbi*.tif")

    # ---------------------------------------------------- dim ---------------------------------------------------------

    dim_dir = os.path.join(mosaics_dir, "SeasonalComposites", "dim")
    print("Dim: ")
    dim_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, dim_dir, 'dim', "*dim*.tif")

    # ---------------------------------------------------- dis ---------------------------------------------------------

    dis_dir = os.path.join(mosaics_dir, "SeasonalComposites", "dis")
    print("Dis: ")
    dis_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, dis_dir, 'dis', "*dis*.tif")

    # ---------------------------------------------------- dja ---------------------------------------------------------

    dja_dir = os.path.join(mosaics_dir, "SeasonalComposites", "dja")
    print("Dja: ")
    dja_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, dja_dir, 'dja', "*dja*.tif")

    # # ---------------------------------------------------- dka -------------------------------------------------------

    dka_dir = os.path.join(mosaics_dir, "fire_scar")
    print("Dka: ")
    dka_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, dka_dir, 'dka', "*dka*.tif")
    # # ---------------------------------------------------- stc -------------------------------------------------------

    stc_dir = os.path.join(mosaics_dir, "structural_formation", "stc_17")
    print("Stc: ")
    stc_export_csv = step1_2_list_of_images.main_routine(
        export_dir_path, stc_dir, 'stc', "*stc*.img")


    print("-"*50)
    print("Zonal stats............")
    print("-" * 50)
    # ------------------------------------------------------------------------------------------------------------------
    # Zonal Stats
    # ------------------------------------------------ h99a2 -----------------------------------------------------------

    h99a2_zonal_stats_output = os.path.join(export_dir_path, 'h99a2_zonal_stats')
    print('h99a2 zonal_stats_output: ', h99a2_zonal_stats_output)

    # no data for persistent green is 0
    no_data = 0
    import step1_4_seasonal_h99a2_zonal_stats
    step1_4_seasonal_h99a2_zonal_stats.main_routine(
        export_dir_path, 'h99a2', h99a2_export_csv, temp_dir_path, geo_df2, no_data)

    # ------------------------------------------------ fpca2 3 bands ---------------------------------------------------

    fpca2_zonal_stats_output = os.path.join(export_dir_path, 'fpca2_zonal_stats')
    print('fpca2 zonal_stats_output: ', fpca2_zonal_stats_output)

    no_data = 0

    import step1_5_seasonal_fpca2_zonal_stats
    step1_5_seasonal_fpca2_zonal_stats.main_routine(
        export_dir_path, 'fpca2', fpca2_export_csv, temp_dir_path, geo_df2, no_data)

    # ---------------------------------------------------- dbi 6 bands working -----------------------------------------

    dbi_zonal_stats_output = os.path.join(export_dir_path, 'dbi_zonal_stats')
    print('dbi zonal_stats_output: ', dbi_zonal_stats_output)

    no_data = 32767

    import step1_6_seasonal_dbi_zonal_stats
    step1_6_seasonal_dbi_zonal_stats.main_routine(
        export_dir_path, 'dbi', dbi_export_csv, temp_dir_path, geo_df2, no_data)

    # ------------------------------------------------ dim 3 bands working ---------------------------------------------

    dim_zonal_stats_output = os.path.join(export_dir_path, 'dim_zonal_stats')
    print('dim zonal_stats_output: ', dim_zonal_stats_output)

    no_data = 0

    import step1_7_seasonal_dim_zonal_stats
    step1_7_seasonal_dim_zonal_stats.main_routine(
        export_dir_path, 'dim', dim_export_csv, temp_dir_path, geo_df2, no_data)

    # -------------------------------------------------- dis classified working ----------------------------------------

    dis_zonal_stats_output = os.path.join(export_dir_path, 'dis_zonal_stats')
    print('dis zonal_stats_output: ', dis_zonal_stats_output)

    no_data = 255

    import step1_8_seasonal_dis_zonal_stats
    step1_8_seasonal_dis_zonal_stats.main_routine(
        export_dir_path, 'dis', dis_export_csv, temp_dir_path, geo_df2, no_data)

    # ------------------------------------------------ dja greyscale working -------------------------------------------

    dja_zonal_stats_output = os.path.join(export_dir_path, 'dja_zonal_stats')
    print('dja zonal_stats_output: ', dja_zonal_stats_output)

    no_data = 0

    import step1_9_seasonal_dja_zonal_stats
    step1_9_seasonal_dja_zonal_stats.main_routine(
        export_dir_path, 'dja', dja_export_csv, temp_dir_path, geo_df2, no_data)

    # ---------------------------------------------------- dka classified working --------------------------------------

    dka_zonal_stats_output = os.path.join(export_dir_path, 'dka_zonal_stats')
    print('dka zonal_stats_output: ', dka_zonal_stats_output)

    no_data = 255

    import step1_10_seasonal_dka_zonal_stats
    step1_10_seasonal_dka_zonal_stats.main_routine(
        export_dir_path, 'dka', dka_export_csv, temp_dir_path, geo_df2, no_data)

    # ---------------------------------------------------- stc classified working --------------------------------------

    print("beginning STC")
    stc_zonal_stats_output = os.path.join(export_dir_path, 'stc_zonal_stats')
    print('stc zonal_stats_output: ', stc_zonal_stats_output)

    no_data = 0

    import step1_11_seasonal_stc_zonal_stats
    step1_11_seasonal_stc_zonal_stats.main_routine(
        export_dir_path, 'stc', stc_export_csv, temp_dir_path, geo_df2, no_data)

    # ---------------------------------------------------- Clean up ----------------------------------------------------

    shutil.rmtree(temp_dir_path)
    print('Temporary directory and its contents has been deleted from your working drive.')
    print(' - ', temp_dir_path)
    print('NT mosaic biomass zonal pipeline is complete.')
    print('goodbye.')


if __name__ == '__main__':
    main_routine()
