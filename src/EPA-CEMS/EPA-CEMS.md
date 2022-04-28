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
