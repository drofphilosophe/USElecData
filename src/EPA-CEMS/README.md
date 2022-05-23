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
2. `downloadFacilityData.py` - NOTE: This currently doesn't work but will hopefully be fixed with the CAMPD API rollout. Instead, download the following bookmarked AMPD query as a CSV. <https://ampd.epa.gov/ampd/#?bookmark=22021>
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
