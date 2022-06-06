######################
## getFacilityData.py
##
## The AMPD interface occasionally reutrns facility data files that are
## not valid CSVs. Usually there is a misalignment in some rows.
## This script reapirs corrupted CSVs by loading them and resaving
## as a gzipped tab-separated value file. The python readers appear
## to be more robust that those in readr to the errors and create
## readable output files.
#######################
import gzip
import os
import io
import yaml
import datetime as dt
import urllib.request
import ssl

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

#API Key
epaCAMPDAPIKey = projectLocalConfig["auth"]["EPA-CAMPD-API-key"]

#Years to process
startYear = int(projectConfig["sources"]["EPA-CEMS"]["start-year"])
endYear = projectConfig["sources"]["EPA-CEMS"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)


#Headers to pass to the server
headers = {
    'Accept' : 'text/csv',
    'Sec-Fetch-Dest' : 'empty',
    'Sec-Fetch-Mode' : 'cors',
    'Sec-Fetch-Site' : 'same-site',
    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.24 Safari/537.36',
    'sec-ch-ua' : '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
    'sec-ch-ua-mobile' : '?0',
    'sec-ch-ua-platform' : '"Windows"',
    'x-api-key' : epaCAMPDAPIKey
}

#Create an SSL context that skips certificate verification
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


for yr in range(startYear,endYear + 1) :

    URL = "https://api.epa.gov/easey/facilities-mgmt/facilities/attributes/stream?&year={year}".format(year=yr)

    print("Getting Facility data for",yr)
    #Construct a request
    req = urllib.request.Request(URL,None,headers)

    with io.BytesIO() as buf :
        print("\tDownloading")
        with urllib.request.urlopen(req,context=ctx) as resp :
            buf.write(resp.read())

        print("\tWriting output")
        buf.seek(0)
        with gzip.open(os.path.join(outPathBase,"FacilityData_{year}.csv.gz".format(year=yr)),"w") as fout :
            fout.write(buf.read())

    raise Exception("STOPPING")
