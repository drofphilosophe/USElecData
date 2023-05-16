######################
## downloadCEMSData.py
##
## This script downloads hourly CEMS
## operation data using the CAMPD
## bulk files interface
#######################
import urllib.request
import gzip
import os
import datetime as dt
import io
import yaml
import hashlib
import csv
import json
import re

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
outPathBase = os.path.join(outputRoot,"data","source","EPA-CEMS","hourly")

#Construct the directory tree if it doesn't already exist
if os.path.isdir(outputRoot) :
    #Check that the data directiry exists
    if not os.path.isdir(outPathBase) :
        os.makedirs(outPathBase)
else :
    raise FileNotFoundError(
    "The data output defined path in the configuration file does not exist\n" +
    "Expecing path: " + outputRoot
    )


########################
## Set the update_all flag
## This will be passed as an argument
## if true
########################
replace_all = False


#Path to EPA hourly CEMS data. Subfolders should be years
api_url = projectConfig["sources"]["EPA-CEMS"]["URL-API"]

#Get the EPA API key
api_key = projectLocalConfig["auth"]["EPA-CAMPD-API-key"]

#Years to process
startYear = int(projectConfig["sources"]["EPA-CEMS"]["start-year"])
endYear = projectConfig["sources"]["EPA-CEMS"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)

HTTPFailMax = 5

#########################
## Request a listing of the available
## bulk files. Its is returned as a JSON file
#########################
req = urllib.request.Request(
    f"{api_url}/camd-services/bulk-files",
    headers={
            "x-api-key" : api_key,
            "Accept" : "application/json"
        }
    )

resp_data = None
with io.StringIO() as buf :
    with urllib.request.urlopen(req) as resp :
        print("Requesting Bulk Data File Listing")
        buf.write(resp.read().decode('utf-8'))

    buf.seek(0)
    print("Parsing Bulk Data JSON File")
    resp_data = json.load(buf)

#Storage for a list of available files
#It will be a dictionary of filename keys with each value being a dictionary
#of file attributes
file_dict = {}

#Define a regular expression that matches quarterly emissions filenames in the bulk file listing
em_filename_pattern = re.compile(r"^emissions-hourly-([0-9]{4})-q([1-4]).csv$")
for x in resp_data :
    filename_match = em_filename_pattern.search(x['filename'])
    if filename_match :
        file_data = {
            x['filename'] : {
                'url' : x['s3Path'],
                'lastUpdate' : dt.datetime.fromisoformat(x['lastUpdated']),
                'size' : int(x['bytes']),
                'year' : int(filename_match[1]),
                'quarter' : int(filename_match[2])
                }
            }
        file_dict.update(file_data)


#################################
## Load our local copy of the download log, if it exists
#################################
download_log_path = os.path.join(outPathBase,"download-log.csv")
download_log = {}
if os.path.isfile(download_log_path) :
    with open(download_log_path,"r") as fin :
        rdr = csv.DictReader(fin)
        for r in rdr :
            #Translate into a nested dict with filename as the key
            r_out = { r['filename'] : {
                'sourceDatetime' : dt.datetime.fromisoformat(r['sourceDatetime']),
                'downloadDatetime' : dt.datetime.fromisoformat(r['downloadDatetime']),
                "year" : r['year'],
                "quarter" : r['quarter'],
                "csv_size" : r['csv_size'],
                "csv_MD5hash" : r['csv_MD5hash']
                }
            }
            download_log.update(r_out)


##############################
## Construct a list of files we need to download
##############################
if replace_all :
    print("Replacing any pre-exisiting files")
    files_to_download = file_dict
else :
    files_to_download = {}
    for fn in file_dict :
        #Start with files that are not in download_log
        if fn not in download_log :
            print(f"No local {fn}")
            files_to_download.update({fn : file_dict[fn]})
        else :
            #Add files that report being updated since their last download
            if file_dict[fn]['lastUpdate'] > download_log[fn]['downloadDatetime'] :
                print(f"Local {fn} is older than remote")
                files_to_download.update({fn : file_dict[fn]})
                

##############################
## Construct a list of files we've processed
##############################
processed_files = {fn : download_log[fn] for fn in download_log if fn not in files_to_download}

##############################
## Tell the user what we're about to do
##############################
print(f"Queued {len(files_to_download)} files for download")
print(f"Skipping {len(processed_files)} files that do not need to be updated")

##################
## Initiate an exception handler that will finish with writing the
## full list of procesed files to the file log
##################
try :
    for fn in files_to_download :
        #Do downloads here
        url = f"{api_url}/bulk-files/{files_to_download[fn]['url']}"
        req = urllib.request.Request(
            url,
            headers={"x-api-key" : api_key}
            )

        print(f"Downloading {url}")
        #Read the file to a buffer
        with io.BytesIO() as buf :

            #When did we initiate download. Set the timezone to UTC to make this
            #a timezone aware datetime
            download_time = dt.datetime.now(dt.timezone.utc)

            success = False
            while success == False :
                try :
                    with urllib.request.urlopen(req) as resp :
                        buf.write(resp.read())
                    success = True
                except :
                    if fail_count >= HTTPFailMax :
                        print("Maximum retrys exceeded. Aborting")
                        raise e
                    else :
                        print("Download failed. Retrying")
                        fail_count+=1

            #How big is the raw file?
            num_bytes = buf.tell()

            #Rewind
            buf.seek(0)

            print("Hashing")
            #Compute a hash of the raw file
            h=hashlib.new('md5')
            h.update(buf.read())
            md5 = h.hexdigest()

            #Rewind
            buf.seek(0)
            
            #Write the buffer to a gzip file
            path_out = os.path.join(outPathBase,fn + ".gz")
            print(f"Writing to {path_out}")
            with gzip.open(path_out,"wb") as zipout :
                zipout.write(buf.read())
                
        #Add the downloaded file to processed_files
        if num_bytes != file_dict[fn]['size'] :
            print(f"Reported file size was {file_dict[fn]['size']} but the downloaded file was {num_bytes} bytes")
            
        file_info ={
            'sourceDatetime' : file_dict[fn]['lastUpdate'],
            'downloadDatetime' : download_time,
            "year" : file_dict[fn]['year'],
            "quarter" : file_dict[fn]['quarter'],
            "csv_size" : num_bytes,
            "csv_MD5hash" : md5
        }
        processed_files.update({fn : file_info})

##########################
## Write the processed files log
## I do work to make sure the dictionary actively
## reflects the state of all processed files
## so this should allow the script to gracefully resume
## from a previous failure
###########################
finally :
    print("Writing the downloaded file log")
    #Write the download log
    with open(download_log_path,"w",newline="") as fout :
        wrtr = csv.DictWriter(
            fout,
            fieldnames=[
                "filename","year","quarter","csv_size",
                "sourceDatetime","downloadDatetime",
                "csv_MD5hash"
                ]
            )
        wrtr.writeheader()
        for fn in processed_files :
            data_line = processed_files[fn]
            data_line['sourceDatetime'] = data_line['sourceDatetime'].isoformat()
            data_line['downloadDatetime'] = data_line['downloadDatetime'].isoformat()
            data_line.update({'filename':fn})
            wrtr.writerow(data_line)
            

            
    
