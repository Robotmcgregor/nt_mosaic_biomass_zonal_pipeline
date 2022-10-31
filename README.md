    
## NT Mosaic Biomass Zonal Pipeline

Pipeline Description: This pipeline creates a 1ha plot from biomass extent point data and extracts zonal statistics from the following Landsat mosaics:

| File Name | Season | Definition | Band Comp | Data Type | 
| :---:  | :---:  | :---: | :---: | :---: |
| h99a2 |  Annual | Canopy Height 99th Percentile |  Greyscale | Continuous |
| fpca2 |  Dry season | Woody Foliage Projective Cover | Greyscale | Continuous |
| dbi | Seasonal  | Surface Reflectance  | 6 band | Continuous|
| dim | Seasonal  | Fractional Cover v2  | 3 band | Discrete (percentage)|
| dis |  Seasonal |   |  Greyscale |Ordinal (1-10)|
| dja |  Seasonal | QLD Persistent Green  | Greyscale | Continuous|
| dka |  Seasonal |  Fractional Cover v3 | 3 band | Discrete (percentage)|
| stc |  Annual | Vegetation Class | Greyscale |Ordinal (1-17)|



## Outputs
- Output 1: Zonal statistic csv for each site per Landsat file type if situated with mosaic boundary.
- Output 2: GDA94 point shapefile.
- Output 2: GDA94 1ha polygon shapefile.
- Output 2: Australian Albers 1ha polygon 1ha shapefile.

## Parameters

input csv - point data in geographics GDA94 with the following features: 

| uid | site    | bio_agb_kg1ha | date |
| :---:   | :---: | :---: | :---: |
| int (unique id) | str(site name)   | float(agb value)   | int(YYYMMDD) |

Landsat mosaic directory:
located here: Z:\Landsat\mosaics
note: script requires the current directory structure, any change to structure can be updated on the initiation script
- step1_1_initiate_fractional_cover_zonal_stats_pipeline.py



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
   
[comment]: <> ( - image_count)

[comment]: <> (    - Integer object that contains the minimum number of Landsat images &#40;per file type&#41; required for zonal stats to run )

[comment]: <> (&#40;default value is 5&#41;.)
      
 - mosaic_dir
    - String object containing the path to the Landsat seasonal mosaic directory (default value is r'Z:\Landsat\mosaic').
   Note: deviation from this structure fill cause the pipeline to fail; however, path changes can be easily made on 
   step1_1_initiate_fractional_cover_zonal_stats_pipeline.py

[comment]: <> ( - no_data)

[comment]: <> (    - Integer object containing the data no data value -- default set to 0.)

