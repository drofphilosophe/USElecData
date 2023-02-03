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
import urllib.error
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
    if not os.path.isdir(outPathBase) :
        print("Creating Output Folder")
        os.makedirs(outPathBase)
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
#For some reason the API fails Python verification
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

#Read the last time this script was run
lastCollectedPath = os.path.join(outPathBase,"facilityDataLastCollected.txt")
lastCollectedTime = None

if os.path.isfile(lastCollectedPath) :
    with open(lastCollectedPath,"r") as infile :
        lastCollectedTime = dt.datetime.fromisoformat(infile.read())
    print("Data last downloaded at",lastCollectedTime)
else :
    #If the file doesn't exist, force recollection of all the files
    lastCollectedTime = dt.datetime(1900,1,1,0)
    print("Unable to find last download time. Downloading all files")




for yr in range(startYear,endYear + 1) :
    outfilePath = os.path.join(outPathBase,"FacilityData_{year}.csv.gz".format(year=yr))

    #If the output file exists and it was downloaded sufficient recently, don't do it again
    #"Sufficiently recent" is at least four months after the end of the year in which the data were contained
    if os.path.isfile(outfilePath) and lastCollectedTime > dt.datetime(yr,12,31) + dt.timedelta(days=124) :
        print("We already have an up-to-date facility file for",yr,". Skipping.")
    else :
        #URL = "https://api.epa.gov/easey/facilities-mgmt/facilities/attributes/stream?&year={year}".format(year=yr)
        URL = f"https://api.epa.gov/easey/streaming-services/facilities/attributes?year={yr}"
        
        print("Getting Facility data for",yr)
        #Construct a request
        req = urllib.request.Request(URL,None,headers)

        with io.BytesIO() as buf :
            print("\tDownloading")
            try :
                with urllib.request.urlopen(req,context=ctx) as resp :
                    buf.write(resp.read())

                print("\tWriting output file")
                buf.seek(0)
                with gzip.open(outfilePath,"w") as fout :
                    fout.write(buf.read())
                    
            except urllib.error.HTTPError as e :
                #The server will respond with 400: Bad Request if data for <yr> does not exist
                if e.code == 400 :
                    print("\tServer responded with 400: Bad Request")
                    if yr == dt.date.today().year :
                        #Prior to the inial release of data for a given year there will be no facility data for that year
                        #Allow this request to fail gracefully
                        print("Missing file is for the current year. This is not unexpected early in the year.")
                    else :
                        #Missing facility information for past years is likley a problem
                        print("Missing file is for a previous year. This will likley lead to errors processing CEMS data.")
                        raise e
                else :
                    #Reraise the exception if it wasn't a Bad Request exception
                    raise e

 #Write out the update time
with open(lastCollectedPath,"w") as outfile :
    outfile.write(dt.datetime.now().isoformat())
