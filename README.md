    
# NT Mosaic Biomass Zonal Pipeline

Pipeline Description: Description: This pipeline comprises of 11 scripts which read in the AGB biomass csv. Pipeline 
converts data to  geo-dataframe and created a 1ha polygon (site) for each point. Once this is complete the pipeline 
runs zonal statistics on the current Landsat mosaic and exports a csv per site into an outputs directory. 

**Note:** Seasonal composites can be 6 band, 3 band, greyscale and classified.
Once pipeline is complete a temporary directory which was created will be deleted from the working drive, 
if script fails the temporary directory requires manual deletion.


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
- **Output 1**: Zonal statistic csv for each site per Landsat file type if situated with mosaic boundary.
- **Output 2**: GDA94 point shapefile.
- **Output 2**: GDA94 1ha polygon shapefile.
- **Output 2**: Australian Albers 1ha polygon 1ha shapefile.

## Parameters

input csv - point data in geographics GDA94 with the following features: 

| uid | site    | bio_agb_kg1ha | date |
| :---:   | :---: | :---: | :---: |
| int (unique id) | str(site name)   | float(agb value)   | int(YYYMMDD) |

Landsat mosaic directory:
located here: Z:\Landsat\mosaics
**Note**: script requires the current directory structure, any change to structure can be updated on the initiation script
- step1_1_initiate_fractional_cover_zonal_stats_pipeline.py



Command arguments:
------------------

 - **tile_grid**
    - String object containing the path to the Landsat tile grid shapefile.

 - **data**:
    - String object containing the file path to the agb biomass csv created from biomass_field_data_clean_v4.ipynb notebook. 
Refer to previous section for csv feature requirements.

 - **export_dir**:
    - String object containing the path to a directory. An export directory tree will be created here, with all outputs 
exported here.
      
 - **mosaic_dir**:
    - String object containing the path to the Landsat seasonal mosaic directory (default value is r'Z:\Landsat\mosaic').
   Note: deviation from this structure fill cause the pipeline to fail; however, path changes can be easily made on 
   step1_1_initiate_fractional_cover_zonal_stats_pipeline.py

