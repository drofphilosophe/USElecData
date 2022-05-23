# EIA-Form860

This folder contains code for loading and cleaning EIA Form 860

## Dependencies

Code in this folder has no external dependencies.

## Build Instructions

To build this dataset run scripts in the following order:

1. `getEIAForm860.py`
2. `loadEIA860_Schedule3_Generators.R`
3. `loadEIA860_Schedule6.R`

The code for processing this dataset is incomplete and additional scripts will
be added with time.

## Output

Scripts in this folder will create output files in `data/out/EIA-Form860`. These
files are:

1. `Form860_Schedule3_Generator` - Annual data on individual generating units
2. `Form860_Schedule6_BoilerGenerator` - Annual data on associations between boilers
and their associated generating units
3. `Form860_Schedule6_BoilerStack` - Annual data on the associations between boilers
and their associated stacks or flues


## Additional files

This directory contains additional files required for processing source the source
data.

### Colspec JSON files
This folder contains one or more column specification files in the JSON format.
Each consists of a single dictionary:

* Keys contain an output file column name as text
* Values are a list of possible column names as for the corresponding data in
source data files

When processing the source data files, the scripts will search through the
value lists in these dictionaries and rename source columns to the key if
a match is found. These file may need to be occasionally updated in the
future if the naming conventions of source file columns change.

The JSON colspec files are:

1. `EIA860_Schedule3_Generators_Colspec.json`  

### Future work

The following legacy scripts provide template code that will eventually be integrated into something usefule for this repo:

1. `loadEnviroAssocEmissionsControlEquipment.R` - Loads information on emissions control equipment associated with each stack/flue from EIA 860 Schedule 6 Part 12. 
