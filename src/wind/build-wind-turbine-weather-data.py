#This script requires an anaconda environment like the following
#conda create -n GeoProcessing -c conda-forge python numpy scipy xarray rasterio rioxarray netCDF4 pyyaml pyarrow
######################
## build-wind-turbine-data.py
##
## Build a dataset of hourly wind speeds and direction for all US wind turbines
#######################
import os
import numpy as np
import xarray as xr
import datetime as dt
import pandas as pd
import pyarrow
import yaml

#########################
## Load and register the configuration
#########################
#Determine the path to this script
scriptPath, _ = os.path.split(os.path.realpath(__file__))
projectRoot = os.path.join(scriptPath,"..","..")

#Load the config.yaml and config_local.yaml configuration Files
with open(os.path.join(projectRoot,"config.yaml")) as yamlin :
    projectConfig = yaml.safe_load(yamlin)

with open(os.path.join(projectRoot,"config_local.yaml")) as yamlin :
    projectLocalConfig = yaml.safe_load(yamlin)

outputRoot = projectLocalConfig["output"]["path"]

#########################
## Path to NOAA RSIG Wind Data
#########################
#We can define it in the local config, otherwise use a default value
rsig_data_path = projectLocalConfig["sources"]["noaa-rsig"]
if rsig_data_path is None :
    rsig_data_path = os.path.join(outputRoot,"data","source","noaa-rsig")

#Define a dictionary of all the wind data we'd like to collect
#It's a dataset name as the key followed by attributes as a list
data_list = {
    "hrrr.wind_10m" : ['wind_velocity','wind_azimuth'],
    "hrrr.wind_80m" : ['wind_velocity','wind_azimuth']
    }

########################
#Load the data file of wind turbine geocodes
#Then extract to vectors
########################
turbines = pd.read_parquet(
    os.path.join(outputRoot,"data","intermediate","wind","wind_turbine_geocodes.parquet")
)

id_list = turbines["orispl.code"].to_numpy()
lat_list = turbines["plant.latitude"].to_numpy()
lon_list = turbines["plant.longitude"].to_numpy()

#Define start and end dates
start_date = projectConfig["sources"]["wind"]["start-date"]
end_date = projectConfig["sources"]["wind"]["end-date"]
#If there is no end date, use yesterday. We'll gracefully skip dates if data don't exist
if end_date is None :
    end_date = dt.date.today() - dt.timedelta(days=1)



#Lists of the indexes of corrdinates for each location
xidx_list = None
yidx_list = None

#Loop through years of the data
for yr in range(start_date.year,end_date.year+1) :
    #Record the path to the output file
    output_filepath = os.path.join(outputRoot,"data","intermediate","wind",f"wind-turbine-weather-data-{yr}.parquet")

    #Check to see if the output file exists
    if os.path.isfile(output_filepath) :
        print(f"Loading pre-existing data for {yr}")
        #Seed the df list with the existing data
        df_list = [pd.read_parquet(output_filepath)]
        #Construct a set of date-measurement tuples in the data
        processed_items = set(zip(df_list[0]["datetime_utc"].dt.date, df_list[0]["measurement"]))
    else :
        #If the file doesn't exist, start with an empty list and no processed items
        df_list = []
        processed_items = set([])

    #Loop through all the approprate days in this year
    this_start_date = max(start_date,dt.date(yr,1,1))
    this_end_date = min(end_date,dt.date(yr,12,31))

    d = this_start_date
    while d <= this_end_date :
        print(f"Processing {d}")

        #Loop through each data item
        for data_item in data_list :
            #Loop through each attribute for this data item
            for attribute_item in data_list[data_item] :
                #Name the measurement implied by data and attribute
                measurement = f"{data_item}_{attribute_item}"

                #Do we already have data for this date?
                if (d,measurement) in processed_items :
                    print(f"\t{measurement} exists. Skipping")
                    continue

                #Otherwise, process this date-measurement
                source_path = os.path.join(
                    rsig_data_path,data_item,attribute_item,
                    f"{d.year:04d}",f"{d.month:02d}",
                    f"{attribute_item}_{d.year:04d}{d.month:02d}{d.day:02d}.nc"
                    )
                
                if not os.path.isfile(source_path) :
                    print(f"No file found at {source_path}")
                    continue

                #Load the netCDF source file
                xrds = xr.open_dataset(source_path)
                    
                #Determine x and y indexes that corrispond to the latitudes and longitudes
                yidx_list = np.searchsorted(xrds["y"].values,lat_list)
                xidx_list = np.searchsorted(xrds["x"].values,lon_list)
                hour_list = xrds["hour"].values.astype(np.int16)
                    
                #Extract the values for every hour at these points
                #This returns an hour x id array
                #Flatten it to a vector
                value_vec = xrds[attribute_item].values[:,yidx_list,xidx_list].flatten()
                
                #Create a vector of the same length that is the corrisponding value from id_list
                #I do this by making a 2D array of IDs then flattening it as above
                id_vec = np.array([[id_list] for h in hour_list]).flatten() 

                #Create a vector of datetimes for each event. This creates a scalar datetime for an hour
                #Then expand it to each ID across columns and then each hour across rows
                time_vec = np.array( 
                    [ [dt.datetime.combine(d,dt.time(h,0,0), tzinfo=dt.timezone.utc)]*len(id_list) for h in hour_list] 
                    ).flatten()

                df = pd.DataFrame({
                    'datetime_utc' : time_vec,
                    'orispl.code' : id_vec,
                    'measurement' : measurement,
                    'value' : value_vec
                })

                df_list += [df]

        #Increment the date
        d+=dt.timedelta(days=1)

    #Write the output file
    if len(df_list) == 1 and len(processed_items) > 0 :
        #In this case, we loaded some prexisting data, but added nothing to it.
        #Don't overwrite the pre-existing data
        print(f"No changes to data from {yr}")  
    elif len(df_list) > 0 :
        print(f"Writing output file for {yr}")
        pd.concat(df_list,ignore_index=True).to_parquet(output_filepath,index=False)
    else :
        print(f"No data for {yr}. Was this expected?")
        print("No output file created")
































        