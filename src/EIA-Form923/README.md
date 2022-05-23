# EIA-Form923

This folder contains code for loading and cleaning data from EIA Form 923

## Dependencies

Code in this foder has no external dependencies

## Build Instructions

To build this dataset run scripts in the following order:

1. `getEIAForm923.py`
2. `loadGenerationAndFuel.R`
3. `loadGenerator.R`

## Output

Scripts in this folder will create the following output files:

1. Source data files in `data/source/EIA-Form923`
2. `eia_923_generation_and_fuel_wide.rds.gz` - A unit-month panel of generation and fuel consumption data

## Future Work
The following files are legacy code that will eventaully be integrated into something useful for this repo:

1. `loadFuelPurchase.R`
2. `loadBoilerFuel.R`
