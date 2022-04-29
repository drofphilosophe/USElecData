######################
## downloadCEMSData.py
##
## This script will download raw CEMS data files
## from the EPA's FTP server. It will construct
## a list of available files, check against the
## local archive of files for newer versions,
## and only download data files that have changed.
#######################
import urllib.request
import zipfile
import os
import re
import datetime as dt
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




#Path to EPA hourly CEMS data. Subfolders should be years
baseURL = projectConfig["sources"]["EPA-CEMS"]["URL-hourly"]

#Years to process
startYear = int(projectConfig["sources"]["EPA-CEMS"]["start-year"])
endYear = projectConfig["sources"]["EPA-CEMS"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)






###################################
## Download CEMS data
###################################
for yr in range(startYear,endYear+1) :
    outPath = os.path.join(outPathBase,str(yr))

    #Check to see if the output folder exist
    if not os.path.isdir(outPath) :
        os.mkdir(outPath)

    #Get a listing of all the files on the server in this folder
    with urllib.request.urlopen(baseURL + "/" + str(yr)) as fileList :
        #Loop through each and extract the last modified time and filename
        fileDict = {}
        for line in fileList.readlines() :
            DateText = line.decode('utf-8')[39:51]
            FileName = line.decode('utf-8')[52:65]
            #Add either time (for previous years) or year (cirrent year) to the DateText
            if ":" in DateText :
                DateText = DateText[0:7] + str(yr) + DateText[6:13]
            else :
                DateText = DateText + " 00:00"

            FileDate = dt.datetime.strptime(DateText,"%b %d %Y %H:%M")

            fileDict.update({FileName.strip() : FileDate})

    #Get a dictionary of the files and their corrisponding modification times in the local directory
    localFileDict = {
        x : dt.datetime.fromtimestamp(
            #min(
            os.path.getmtime(os.path.join(outPath,x))
            #os.path.getctime(os.path.join(outPath,x)))
            )
        for x in os.listdir(outPath)
        if os.path.isfile(os.path.join(outPath,x))
        }

    #Loop through each of the files on the FTP server
    for f in fileDict.keys() :
        print("Processing", f)
        #Set a flag to download data
        doIDownload = False
        #Check to see if we have the file
        if f in localFileDict :
            #If so is the one on the server newer?
            if fileDict[f] > localFileDict[f] :
                print("\tNew version on server.")
                print("\t\tLocal Date:",localFileDict[f])
                print("\t\tRemote Date:",fileDict[f])
                print("\tDownloading")
                doIDownload = True
            else :
                print("\tLocal copy is newer. No download")
        else :
            #Local file doesn't exist. Download it
            print("\tLocal version does not exist. Downloading")
            doIDownload = True

        if doIDownload :
             print("\tDownloading file",f)
             #Construct the URL to the file
             URL = baseURL + "/" + str(yr) + "/" + f

             #Get the URL data and read it into a buffer
             with io.BytesIO() as buf :
                with urllib.request.urlopen(URL) as req :
                    buf.write(req.read())
                #rewind the buffer
                buf.seek(0)

                ##Save the downloaded data. We download a zip
                ##open it and then resave it as a zip. Why
                ##go through this process? First I want
                ##to check the file is structured the way I expect
                ##Second, this verifies the integreity of the zip file.

                #The source file is a zip file. open it in a context handler
                with zipfile.ZipFile(buf) as zipin :
                    csv_name = os.path.splitext(f)[0] + ".csv"
                    with zipin.open(csv_name,"r") as fin :
                        #Open an output file. It is a zip archive
                        print("Writing File",f)
                        with zipfile.ZipFile(os.path.join(outPath,f),"w") as zipout :
                            with zipout.open(csv_name,"w") as fout :
                                #Flush the buffer to the file
                                fout.write(fin.read())
        else :
            print("We already have an up-to-date version of",f)
