#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description: This pipeline comprises of 12 scripts which read in the Rangeland Monitoring Branch odk instances
{instance names, odk_output.csv and ras_odk_output.csv: format, .csv: location, directory}
Outputs are files to a temporary directory located in your working directory (deleted at script completion),
or to an export directory located a the location specified by command argument (--export_dir).
Final outputs are files to their respective property sub-directories within the Pastoral_Districts directory located in
the Rangeland Working Directory.


step1_1_initiate_fractional_cover_zonal_stats_pipeline.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. deletes the temporary directory and its contents once the pipeline has completed.


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
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

--tile_grid: str
string object containing the path to the Landsat tile grid shapefile.

--directory_odk: str
string object containing the path to the directory housing the odk files to be processed - the directory can contain 1
to infinity odk outputs.
Note: output runtime is approximately 1 hour using the remote desktop or approximately  3 hours using your laptop
(800 FractionalCover images).

--export_dir: str
string object containing the location of the destination output folder and contents(i.e. 'C:Desktop/odk/YEAR')
NOTE1: the script will search for a duplicate folder and delete it if found.
NOTE2: the folder created is titled (YYYYMMDD_TIME) to avoid the accidental deletion of data due to poor naming
conventions.

--image_count
integer object that contains the minimum number of Landsat images (per tile) required for the fractional cover
zonal stats -- default value set to 800.

--landsat_dir: str
string object containing the path to the Landsat Directory -- default value set to r'Z:\Landsat\wrs2'.

--no_data: int
ineger object containing the Landsat Fractional Cover no data value -- default set to 0.

--rainfall_dir: str
string object containing the pathway to the rainfall image directory -- default set to r'Z:\Scratch\mcintyred\Rainfall'.

--search_criteria1: str
string object containing the end part of the filename search criteria for the Fractional Cover Landsat images.
-- default set to 'dilm2_zstdmask.img'

--search_criteria2: str
string object from the concatenation of the end part of the filename search criteria for the Fractional Cover
Landsat images. -- default set to 'dilm3_zstdmask.img'

--search_criteria3: str
string object from the concatenation of the end part of the filename search criteria for the QLD Rainfall images.
-- default set to '.img'

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
                   default=r'Z:\Scratch\Zonal_Stats_Pipeline\non_rmb_fractional_cover_zonal_stats\outputs')

    p.add_argument('-i', '--image_count', type=int,
                   help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
                   default=100)

    p.add_argument('-l', '--mosaics_dir', help="The NT seasonal mosaics directory path",
                   default=r"Z:\Landsat\mosaics")

    p.add_argument('-n', '--no_data', help="Enter the Landsat Fractional Cover no data value (i.e. 0)",
                   default=0)


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

    # rainfall_output_dir = (export_dir_path + '\\rainfall')
    # os.mkdir(rainfall_output_dir)

    # dil_tile_status_dir = (export_dir_path + '\\dil_tile_status')
    # os.mkdir(dil_tile_status_dir)
    # #
    # ref_tile_status_dir = (export_dir_path + '\\ref_tile_status')
    # os.mkdir(ref_tile_status_dir)
    #
    # dbg_tile_status_dir = (export_dir_path + '\\dbg_tile_status')
    # os.mkdir(dbg_tile_status_dir)
    #
    # th_tile_status_dir = (export_dir_path + '\\th_tile_status')
    # os.mkdir(th_tile_status_dir)
    #
    # pg_tile_status_dir = (export_dir_path + '\\pg_tile_status')
    # os.mkdir(pg_tile_status_dir)
    #
    # dp0_tile_status_dir = (export_dir_path + '\\dp0_tile_status')
    # os.mkdir(dp0_tile_status_dir)
    #
    # dp1_tile_status_dir = (export_dir_path + '\\dp1_tile_status')
    # os.mkdir(dp1_tile_status_dir)

    # # ----------------------------------------------------------------------
    #
    # dil_tile_for_processing_dir = (dil_tile_status_dir + '\\dil_for_processing')
    # os.mkdir(dil_tile_for_processing_dir)
    # #
    # ref_tile_for_processing_dir = (ref_tile_status_dir + '\\ref_for_processing')
    # os.mkdir(ref_tile_for_processing_dir)
    # #
    #
    # dbg_tile_for_processing_dir = (dbg_tile_status_dir + '\\dbg_for_processing')
    # os.mkdir(dbg_tile_for_processing_dir)

    # th_tile_for_processing_dir = (th_tile_status_dir + '\\th_for_processing')
    # os.mkdir(th_tile_for_processing_dir)
    #
    # pg_tile_for_processing_dir = (pg_tile_status_dir + '\\pg_for_processing')
    # os.mkdir(pg_tile_for_processing_dir)
    #
    # dp0_tile_for_processing_dir = (dp0_tile_status_dir + '\\dp0_for_processing')
    # os.mkdir(dp0_tile_for_processing_dir)
    # #
    #
    # dp1_tile_for_processing_dir = (dp1_tile_status_dir + '\\dp1_for_processing')
    # os.mkdir(dp1_tile_for_processing_dir)

    # # -----------------------------------------------------------------------
    #
    # dil_insuf_files_dir = (dil_tile_status_dir + '\\dil_insufficient_files')
    # os.mkdir(dil_insuf_files_dir)
    # #
    # ref_insuf_files_dir = (ref_tile_status_dir + '\\ref_insufficient_files')
    # os.mkdir(ref_insuf_files_dir)
    #
    # dbg_insuf_files_dir = (dbg_tile_status_dir + '\\dbg_insufficient_files')
    # os.mkdir(dbg_insuf_files_dir)
    # #
    # th_insuf_files_dir = (th_tile_status_dir + '\\th_insufficient_files')
    # os.mkdir(th_insuf_files_dir)
    #
    # pg_insuf_files_dir = (pg_tile_status_dir + '\\pg_insufficient_files')
    # os.mkdir(pg_insuf_files_dir)
    #
    # dp0_insuf_files_dir = (dp0_tile_status_dir + '\\dp0_insufficient_files')
    # os.mkdir(dp0_insuf_files_dir)
    #
    # dp1_insuf_files_dir = (dp1_tile_status_dir + '\\dp1_insufficient_files')
    # os.mkdir(dp1_insuf_files_dir)
    # #
    # # ------------------------------------------------------------------------
    # #
    # dil_stat_list_dir = dil_tile_status_dir + '\\dil_tile_status_lists'
    # os.mkdir(dil_stat_list_dir)
    # #
    # ref_stat_list_dir = ref_tile_status_dir + '\\ref_tile_status_lists'
    # os.mkdir(ref_stat_list_dir)
    #
    # dbg_stat_list_dir = dbg_tile_status_dir + '\\dbg_tile_status_lists'
    # os.mkdir(dbg_stat_list_dir)
    # #
    # # th_stat_list_dir = th_tile_status_dir + '\\th_tile_status_lists'
    # # os.mkdir(th_stat_list_dir)
    # #
    # # pg_stat_list_dir = pg_tile_status_dir + '\\pg_tile_status_lists'
    # # os.mkdir(pg_stat_list_dir)
    # #
    # dp0_stat_list_dir = dp0_tile_status_dir + '\\dp0_tile_status_lists'
    # os.mkdir(dp0_stat_list_dir)
    # #
    #
    # dp1_stat_list_dir = dp1_tile_status_dir + '\\dp1_tile_status_lists'
    # os.mkdir(dp1_stat_list_dir)

    # # -------------------------------------------------------------------------
    #
    # plot_dir = export_dir_path + '\\plots'
    # os.mkdir(plot_dir)
    #
    # interactive_outputs = plot_dir + '\\interactive'
    # os.mkdir(interactive_outputs)
    #
    # final_plot_outputs = export_dir_path + '\\final_plots'
    # os.mkdir(final_plot_outputs)
    #
    # final_interactive_outputs = export_dir_path + '\\final_interactive'
    # os.mkdir(final_interactive_outputs)
    #
    # dil_zonal_stats_output_dir = (export_dir_path + '\\dil_zonal_stats')
    # os.mkdir(dil_zonal_stats_output_dir)
    #
    # ref_zonal_stats_output_dir = (export_dir_path + '\\ref_zonal_stats')
    # os.mkdir(ref_zonal_stats_output_dir)
    #
    # dbg_zonal_stats_output_dir = (export_dir_path + '\\dbg_zonal_stats')
    # os.mkdir(dbg_zonal_stats_output_dir)
    # #
    # th_zonal_stats_output_dir = (export_dir_path + '\\th_zonal_stats')
    # os.mkdir(th_zonal_stats_output_dir)
    #
    # pg_zonal_stats_output_dir = (export_dir_path + '\\pg_zonal_stats')
    # os.mkdir(pg_zonal_stats_output_dir)
    #
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
     - stc

    Output 1: Zonal statistic csv for each sites per file type if situated with mosaic boundary.
    Output 2: GDA94 point shapefile
    Output 2: GDA94 1ha polygon shapefile
    Output 2: Australian Albers 1ha polygon 1ha shapefile."""

    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    data = cmd_args.data
    export_dir = cmd_args.export_dir
    mosaics_dir = cmd_args.mosaics_dir
    no_data = int(cmd_args.no_data)
    image_count = int(cmd_args.image_count)

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
    #geo_df2.to_file(os.path.join(export_dir_path, "biomass_1ha.shp"))

    shapefile_path = os.path.join(export_dir_path, "biomass_1ha_all_sites.shp")
    geo_df2.to_file(os.path.join(shapefile_path),
                    driver="ESRI Shapefile")

    print("Exported shapefile: ", shapefile_path)
    # ----------------------------------------------------- h99 --------------------------------------------------------

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
