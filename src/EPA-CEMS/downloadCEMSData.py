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



#################
## getRemoteFileList
##
## Get a list of remote files from the FTP server
#################
def getRemoteFileList(URL) :
    #Compile a regex that will match valid CEMS filenames
    fnPattern = re.compile(fr"{yr}[A-Za-z][A-Za-z][0-1][0-9]\.zip")

    #Create a dictionary in which to return results
    remoteFileDict = {}

    #Get a listing of all the files on the server in this folder
    with urllib.request.urlopen(URL) as fileList :
        #Loop through each and extract the last modified time and filename
        for line in fileList.readlines() :
            
            DateText = line.decode('utf-8')[39:51]
            FileName = line.decode('utf-8')[52:65]

            #Check that the file name matches the pattern of CEMS file names
            if fnPattern.search(FileName) :
                #Add either time (for previous years) or year (current year) to the DateText
                if ":" in DateText :
                    DateText = DateText[0:7] + str(yr) + DateText[6:13]
                else :
                    DateText = DateText + " 00:00"

                FileDate = dt.datetime.strptime(DateText,"%b %d %Y %H:%M")

                remoteFileDict.update({FileName.strip() : FileDate})
            else :
                print("Skipping",FileName,"as a non-conforming file name pattern")    
    return remoteFileDict

##############
## getLocalFileList
##
## Get a dictionary of local files
###############
def getLocalFileList(path) :

    if not os.path.isfile(os.path.join(path,"SourceFileLog.csv")) :
        if len(os.listfiles(path)) > 0 :
            print("SourceFileLog.csv is not found in the hourly data folder")
            print("But there are pre-existing files in the hourly data folder")
            print("This is unexpected and means your source data are out-of-sync")
            print("You should remove pre-existing files or run rectonSourceFileDB.py to")
            print("Reconstruct the source file database.")
            raise FileNotFound("SourceFileLog.csv Not found")
        else :
            print("This is an initial build of the source data")
            print("Creating SourceFileLog.csv")
            localFileDict = {}
    else :
        localFileDict = {}
        with open(os.path.join(path,"SourceFileLog.csv"),"r") as csvin :
            reader = csv.DictReader(csvin)

            for row in reader :
                localFileEntry = {
                    row['filename'] :
                    ( row['Year'],
                      dt.datetime.fromisoformat(row['download_timestamp']),
                      row['md5hash']
                      )
                    }
                localFileDict.update(localFileEntry)
            
        return localFileDict

#####################
## processRemoteFile
#####################
def processRemoteFile(remoteDB,localDB,outPath,yr,fn) :
    print("Processing",fn)
    #A flag to download the file
    doIDownload = False

    #Default that local file attribtutes will be unchanged
    newAttribs = localDB[fn]

    #Check to see if the file is in the local database
    if fn in localDB :
        #Is the remote copy newer?
        if remoteDB[fn] > localDB[fn][1] :
            doIDownload = True            
    else :
        doIDownload = True
        
    if doIDownload :
        #Download the file if needed
        URL = baseURL + "/" + str(yr) + "/" + fn
        with io.BytesIO() as buf :
            with urllib.request.urlopen(URL) as req :
                buf.write(req.read())
                    
            #rewind the buffer
            buf.seek(0)

            #Compute the MD5 hash
            h=hashlib.new('md5')
            h.update(buf.read())
            md5 = h.hexdigest()

            #Did the hash change?
            if md5 == localDB[fn][2] :
                print("\tFile date changed, but the hash did not. Updating the file date")
                newAttribs = (yr,remoteDB[fn],md5)
                
            else :
                print("\tNewer file on remote. Updating")
                buf.seek(0)
                
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
                newAttribs = (yr,remoteDB[fn],md5)
    else :
        print("\tLocal file is newer. No update")
                
    if fn in localDB :
        localDB[fn] = newAttribs
    else :
        localDB.update({fn : newAttribs})
        
            
def writeLocalFileDB(outPathBase,localDB) :
    print("Writing SourceFileLog.csv")
    with open(os.path.join(outPathBase,"SourceFileLog.csv"),"w") as csvout :
        csvout.write("Year,filename,download_timestamp,md5hash\n")
        for row in localFileDict.keys() :
            csvout.write(
                str(localFileDict[row][0]) + "," +
                row + "," + 
                dt.datetime.isoformat(localFileDict[row][1]) + "," +
                str(localFileDict[row][2]) + "\n"
            )   




#Get a list of local files from the source file DB
localFileDict = getLocalFileList(outPathBase)

###################################
## Download CEMS data
###################################
try :
    for yr in range(startYear,endYear+1) :
        outPath = os.path.join(outPathBase,str(yr))

        #Check to see if the output folder exist
        if not os.path.isdir(outPath) :
            os.mkdir(outPath)

        #Get a list of files for this year from the remote
        remoteFileDict = getRemoteFileList(baseURL + "/" + str(yr))

        
        #Loop through each of the files on the FTP server
        for f in remoteFileDict.keys() :
            processRemoteFile(remoteFileDict,localFileDict,outPath,yr,f)
except Exception as e :
    raise e
finally :
    ##Write out SourceFileLog
    writeLocalFileDB(outPathBase,localFileDict)
            
    
