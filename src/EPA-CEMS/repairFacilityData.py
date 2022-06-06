######################
## DEPRICATED - repairFacilityData.py
##
## The AMPD interface occasionally reutrns facility data files that are
## not valid CSVs. Usually there is a misalignment in some rows.
## This script reapirs corrupted CSVs by loading them and resaving
## as a gzipped tab-separated value file. The python readers appear
## to be more robust that those in readr to the errors and create
## readable output files.
##
## With the full release of the CAMPD API this error no longer occurs
## and this script is depricated.
#######################
import zipfile
import gzip
import os
import io
import yaml
import csv
import datetime as dt

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
outPathBase = os.path.join(outputRoot,"data","source","EPA-CEMS","facility_data")

#Construct the directory tree if it doesn't already exist
if os.path.isdir(outputRoot) :
    pass
else :
    raise FileNotFoundError(
    "The data output defined path in the configuration file does not exist\n" +
    "Expecing path: " + outputRoot
    )




#Path to EPA hourly CEMS data. Subfolders should be years
baseURL = projectConfig["sources"]["EPA-CEMS"]["URL-hourly"]

#Years to process
startYear = int(projectConfig["sources"]["EPA-CEMS"]["start-year"])
endYear = projectConfig["sources"]["EPA-CEMS"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)

with zipfile.ZipFile(os.path.join(outPathBase,"FacilityData.zip")) as zipin :
    filename = [x for x in zipin.namelist() if "facility" in x][0]
    #Use the CSV reader to parse the file
    with io.TextIOWrapper(zipin.open(filename,"r"),encoding='utf=8') as filein :
        #Open a gzipped TSV file for output
        with io.TextIOWrapper(gzip.open(os.path.join(outPathBase,"FacilityData.txt.gz"),"w"),encoding='utf-8',newline='') as fout :
            reader = csv.DictReader(filein,dialect='excel')
            writer = csv.DictWriter(fout,fieldnames=reader.fieldnames,delimiter="\t",quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for row in reader :
                #Remove the terminal key in row
                row.pop(None)
                writer.writerow(row)
                
    #with io.TextIOWrapper(zipin.open(filename,"r"),encoding="utf-8") as filein :
    #    for row in filein.readlines() :
    #        print(row)
            
