######################
## getTZInfo.py
##
## Download a vector data file of world time zones maintained
## in the github repo of evansiroky/timezone-boundary-builder
#######################
import urllib.request
import zipfile
import os
import io
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
outputFolder = os.path.join(outputRoot,"data","source","tz-info")
#Construct the directory tree if it doesn't already exist
if os.path.isdir(outputRoot) :
    if not os.path.isdir(outputFolder) :
        os.makedirs(outputFolder)

else :
    raise FileNotFoundError(
    "The data output defined path in the configuration file does not exist\n" +
    "Expecing path: " + outputRoot
    )

#Path to EPA hourly CEMS data. Subfolders should be years
repositoryName = projectConfig["sources"]["tz-info"]["repository-name"]
repositoryVersion = projectConfig["sources"]["tz-info"]["release"]


############################
## Download the geoJSON version of the
## time zone spatial Data
############################
URL = f"https://github.com/{repositoryName}/releases/download/{repositoryVersion}/timezones-with-oceans.geojson.zip"

with io.BytesIO() as buf :
    print("Downloading")
    with urllib.request.urlopen(URL) as req :
        buf.write(req.read())
    buf.seek(0)
    print("Unzipping")
    with zipfile.ZipFile(buf) as archive :
        with archive.open("combined-with-oceans.json") as zipin :
            print("Writing local file")
            with open(os.path.join(outputFolder,"tz-geodata-combined-with-oceans.json"),"wb") as zipout :
                zipout.write(zipin.read())
