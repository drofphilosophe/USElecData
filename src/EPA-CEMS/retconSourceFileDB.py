######################
## retconSourceFileDB.py
##
## This script constructs the CEMS raw data SourceFileLog.csv from an existing
## datastore. It assumes a file's last-modified time as the time it was downloaded
## which may be unreliable on cloud storage or if a datastore is moved.
##
## This script shouldn't need to be run in the course of building the EPA-CEMS data
## I built it to construct SourceFileLog.csv after-the-fact.
#######################
import urllib.request
import zipfile
import os
import re
import datetime as dt
import io
import yaml
import hashlib
import csv

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
sourcePathBase = os.path.join(outputRoot,"data","source","EPA-CEMS","hourly")

#Construct the directory tree if it doesn't already exist
if not os.path.isdir(outputRoot) :
    raise FileNotFoundError(
    "The data output defined path in the configuration file does not exist\n" +
    "Expecing path: " + outputRoot
    )

#Years to process
startYear = int(projectConfig["sources"]["EPA-CEMS"]["start-year"])
endYear = projectConfig["sources"]["EPA-CEMS"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)



##################
## Loop through each year of data
## and compile information on the sourcefile
##################
#File info will be a list of dictionaries
#Containing year, file name, a download timestamp, and an MD5 hash
fileInfo = []
for yr in range(startYear,endYear+1) :
    print("Processing",yr)
    sourcePath  = os.path.join(sourcePathBase,str(yr))

    if os.path.isdir(sourcePath) :
        #Obtain a list of files
        fileList = [x for x in os.listdir(sourcePath) if os.path.isfile(os.path.join(sourcePath,x))]

        #Loop through each file
        for f in fileList :

            #Hash the file
            h = hashlib.new('md5')
            with open(os.path.join(sourcePath,f),"rb") as fin :
                h.update(fin.read())
            md5 = h.hexdigest()

            #Get the file's last modified timestamp
            fileTimestamp = dt.datetime.fromtimestamp(os.path.getmtime(os.path.join(sourcePath,f)))
            
            #Save info about the file to a dictionary
            thisFileInfo = {
                'Year' : yr,
                'filename' : f,
                'download_timestamp' : dt.datetime.strftime(fileTimestamp,"%Y-%m-%dT%H:%M:%S%z"),
                'md5hash' : md5
            }
            fileInfo.append(thisFileInfo)

#Write the fileList out as a CSV
with open(os.path.join(sourcePathBase,"SourceFileLog.csv"),"w",newline='') as csvout :
    fieldnames = thisFileInfo.keys()
    writer = csv.DictWriter(csvout,fieldnames=fieldnames)
    writer.writeheader()
    for f in fileInfo :
        writer.writerow(f)

        
    
