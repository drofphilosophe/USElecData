#Download and unzip all of EIA Form 860. The program will delete existing folders
#and overwrite them. There are "utility" and "non-utility" files from 1998 to 2000.
#Account for that weirdness as well.
import urllib.request
import urllib.parse
import io
import os
import zipfile
import shutil
import yaml
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
outPathBase = os.path.join(outputRoot,"data","source","EIA-Form860")

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
baseURL = projectConfig["sources"]["EIA-Form860"]["URL"]

#Years to process
startYear = int(projectConfig["sources"]["EIA-Form860"]["start-year"])
endYear = projectConfig["sources"]["EIA-Form860"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)

    

##################
## You shouldn't need to change anything below this line
##################
#####################################
# downloadAndExtract - Downloads a ZIP archive and extracts it to the specified folder
#####################################
def downloadAndExtract(url, dest, ketchamate=False) : #Download and extract ZIP archive
    #Check if the target folder exists. If so, remove it
    DoIProcess = True
    if os.path.exists(dest) :
        if ketchamate :
            print("\tRemoving Folder")
            shutil.rmtree(dest)
            DoIProcess = True
        else :
            print("\tFolder exists. Skipping.")
            DoIProcess = False
    else :
        DoIProcess = True

    if DoIProcess :
        #Make the Target Directory
        print("\tCreating Folder")
        os.mkdir(dest)

        #Request the URL
        response = urllib.request.urlopen(url)

        print("\tExtracting data from archive...")
        #Convert the response to a file-like object
        zipFile = io.BytesIO()
        zipFile.write(response.read())

        #Convert zipFile to a ZipFile object
        archive = zipfile.ZipFile(zipFile, 'r')

        #Get list of zipFile contents
        archiveNameList = archive.namelist()

        #Extract all of the files in archive
        for fileName in archiveNameList :
            archive.extract(fileName, path=dest)



        #Clean up
        archive.close()
        zipFile.close()
        response.close()

########################################################
##MAIN PROGRAM
########################################################
#define the list of years to download
yearList = range(startYear, endYear+1)

for yr in yearList :
    print("Year: " + str(yr))
    destFolder = outPathBase + '\\' + str(yr)


    #Let us know what is happening
    print("\tDownloading " + str(yr) + "....")

    #There are three different formats of files. Treat them differently
    if yr < 1998 :
        downloadAndExtract(baseURL + "/eia860a/eia860a" + str(yr) + ".zip", outPathBase + "\\" + str(yr) + "a")
    elif yr < 2001 :
        downloadAndExtract(baseURL + "/eia860a/eia860a" + str(yr) + ".zip", outPathBase + "\\" + str(yr) + "a")
        downloadAndExtract(baseURL + "/eia860b/eia860b" + str(yr) + ".zip", outPathBase + "\\" + str(yr) + "b")
    else :
        try :
            downloadAndExtract(baseURL + "/archive/xls/eia860" + str(yr) + ".zip", outPathBase + "\\" + str(yr))
        except zipfile.BadZipFile as e :
            downloadAndExtract(baseURL + "/xls/eia860" + str(yr) + ".zip", outPathBase + "\\" + str(yr))
