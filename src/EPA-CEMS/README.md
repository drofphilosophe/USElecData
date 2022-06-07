# EPA-CEMS

This folder contains code for downloading and cleaning the EPA's CEMS dataset. 

## Dependencies

Code in this foder depends on the following datasets or external files:
* **tz_data** - A shapefile of world time zones. It can be  obtained from <http://efele.net/maps/tz/world/>.
* **EIA-Form923** - Fuel consumption and electriciy generation from the US Energy Information Adminitration Form 923.
* **EIA-Form860** - US electricity generator data from the US Energy Information Administration Form 860.

## Build Instructions

To build this dataset run scripts in the following order:

1. `downloadCEMSData.py`
2. `downloadFacilityData.py`
3. `loadFacilityData.R`
4. `loadCEMSData.R`
5. `CEMStoEIAMap.R`
6. `netToGrossCalculation.R`

## Output

Scripts in this folder will create the following output files:
1. Source hourly CEMS data files in `data/source/EPA-CEMS/hourly`
2. A data file of CEMS facility attributes in `data/out/EPA-CEMS/facility_data`
3. Hourly, daily, and monthly genetation, fuel consumption, and emissions by CEMS unit in `data/out/EPA-CEMS/hourly`, `data/out/EPA-CEMS/daily`, and `data/out/EPA-CEMS/monthly`, respectively
4. Ratios of Net Generation (as measured by EIA 923) to reported CEMS Gross Generation by CEMS unit and month in `data/out/crosswalks/rds/EIANet_to_CEMSGross_Ratios.R`. Note that while true net generation should always be less than gross, many generators do not report gross generation from generating units that are not connected to emissions control equipment, e.g., the steam turbine part of a combined cycle gas turbine. EIA data includes net generation from all generating units, and there are many cases where the net generation reported to EIA exceeds the gross generation reported to CEMS. 

## Other Files

This folder contains the following additional files required to build a complete dataset:
1. `legacyFacilityData.csv.gz` - This is an AMPD facility data file in gzipped CSV format that I downloaded in early 2022 and modified to match the modern CAMPD format. Some facilities reporting into CEMS are no longer included in the CAMPD facility data and this file contains information for those facilites. Unfortunately, there is no way to reconstruct this information from modern primary sources. This file is loaded by `loadFacilityData.R` to augment the facility data from CAMPD. Information is only used when there is no corrisponding entry for a generating unit in the CAMPD facility data files. 
2. `retconSourceFileDB.py` - A python script that will construct the source data file listing from an existing folder containing CEMS source data. `downloadCEMSData.py` constructs/modifies the source file listing as it downloads files and `loadCEMSData.R` reads it to determine which files need to be procesed. Use this script to construct a source file listing on data you've already downloaded. Anyone building the data from scratch will not need to run this script.
3. `repairFacilityData.py` - The AMPD interface would occasionally return facility data CSV files that `readr::read_csv` could not properly parse. This Python script would load and resave the CSV which resolved the parsing issue. This does not appear to be a problem with facility data files returned by the CAMPD API. Thus, this file is depricated and will be removed from future releases. 
