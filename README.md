    
## NT Mosaic Biomass Zonal Pipeline

Pipeline Description: This pipeline creates a 1ha plot from biomass extent point data and extracts zonal statistics from the following Landsat mosaics:
- h99a2
- fpca2
- dbi
- dim
- dis
- dja
- dka
- stc

## Outputs
- Output 1: Zonal statistic csv for each site per Landsat file type if situated with mosaic boundary.
- Output 2: GDA94 point shapefile.
- Output 2: GDA94 1ha polygon shapefile.
- Output 2: Australian Albers 1ha polygon 1ha shapefile.

## Parameters

input csv - point data in geographics GDA94 with the following features: 

| uid | site    | bio_agb_kg1ha |
| :---:   | :---: | :---: |
| int (unique id) | str(site name)   | float(agb value)   |

Landsat mosaic directory:
located here: Z:\Landsat\mosaics
note: script requires the current directory structure, any change to structure can be updated on the initiation script
- step1_1_initiate_fractional_cover_zonal_stats_pipeline.py