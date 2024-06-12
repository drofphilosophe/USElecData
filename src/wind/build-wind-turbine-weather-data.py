#This script requires an anaconda environment like the following
#conda create -n GeoProcessing -c conda-forge python numpy scipy xarray rasterio rioxarray netCDF4
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


#Init the date counter. We'll start one day early
#Because we increment right off the bat
d = start_date - dt.timedelta(days=1)

#Init a list of dataframes to hold the output data
df_list = []

#Loop through each date
prev_year = None
while d <= end_date :
    #Increment the counter
    d+=dt.timedelta(days=1)
    print(f"Processing {d}")

    #######################
    #Write an output file if we're moving to a new year or we've processed the final date
    #######################
    #Our intial condition is prev_year = None
    if prev_year is None :
        prev_year = d.year
    elif d.year != prev_year or d > end_date :
        if len(df_list) > 0 :
            print(f"Writing output file for {prev_year}")
            pd.concat(df_list,ignore_index=True).to_parquet(
                os.path.join(outputRoot,"data","intermediate","wind",f"wind-turbine-weather-data-{prev_year}.parquet"),
                index=False)
        else :
            print(f"No data for {prev_year}. Was this expected?")
            print("No output file created")

        #Reset the df_list
        df_list = []
        prev_year = d.year
        
        #If we're past the end date, terminate
        #Exiting this while loop is sufficient
        if d > end_date :
            break

    #Loop through each data item
    for data_item in data_list :
        #Loop through each attribute for this data item
        for attribute_item in data_list[data_item] :
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
                'measurement' : f"{data_item}_{attribute_item}",
                'value' : value_vec
            })

            df_list += [df]




        