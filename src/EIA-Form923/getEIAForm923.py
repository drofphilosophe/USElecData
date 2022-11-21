#Download and unzip all of EIA Form 923/920/906. The program does not remove exitsing files and folders.
#If you want to replace/update a file you need to delete it and rerun the program.
#There are "utility" and "non-utility" files for some timeframe.
#Account for that weirdness as well.
import urllib.request
import urllib.parse
import io
import os
import zipfile
import shutil
import datetime as dt
import pytz
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
outPathBase = os.path.join(outputRoot,"data","source","EIA-Form923")

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
baseURL = projectConfig["sources"]["EIA-Form923"]["URL"]

#Years to process
startYear = int(projectConfig["sources"]["EIA-Form923"]["start-year"])
endYear = projectConfig["sources"]["EIA-Form923"]["end-year"]
if endYear is None or endYear.strip() == "" :
    endYear = dt.date.today().year
else :
    endYear = int(endYear)

    
###########################
## downloadFile - Downloads an Excel File and saves to specified folder
###########################
def downloadExcel(url, dest) :
    #Assume that we've already downloaded data if the destination folder
    #exists AND there is at least one file in it. And short circuits, so
    #this shouldn't generate exceptions if dest doesn't exist
    if os.path.exists(dest) and len(os.listdir(dest)) > 0:
        print("File exists. Skipping.\n")
    else :
        print("\tI ask server for: " + url)
        with io.BytesIO() as buf :
            with urllib.request.urlopen(url) as response :
                print("\tServer responds: " + response.headers['content-type'] )

                if response.headers['content-type'] == "application/vnd.ms-excel" :
                    #Write the excel file to a buffer
                    print("\tSaving Excel File")
                    buf.write(response.read())
                    
            #The buffer will be at position zero if we didn't get an Excel file         
            if buf.tell() > 0 :
                #Rewind the buffer
                buf.seek(0)
                #Dump the serer response to an appropriately-named binary file
                with open(dest,"wb") as XLFile :
                    XLFile.write(buf.read())
            else :
                print("No suitable file type found.")

#####################################
# downloadAndExtract - Downloads a ZIP archive and extracts it to the specified folder
#####################################
def downloadAndExtract(url, dest) : #Download and extract ZIP archive
    #Assume that we've already downloaded data if the destination folder
    #exists AND there is at least one file in it. And short circuits, so
    #this shouldn't generate exceptions if dest doesn't exist
    if os.path.exists(dest) and len(os.listdir(dest)) > 0:
         print("Folder already exists. Skipping.\n")
    else :

        #Make the Target Directory
        print("\tCreating Folder")
        os.mkdir(dest)

        with io.BytesIO() as bufout :
            with io.BytesIO() as bufin :
                print("\tDownloading",url)
                with urllib.request.urlopen(url) as response :
                    print("\tExtracting data from archive...")
                    bufin.write(response.read())
                bufin.seek(0)

                with zipfile.ZipFile(bufin) as archive :
                    #Get list of zipFile contents
                    archiveNameList = archive.namelist()
                    #Read each file from the archive and write it to the destination folder
                    for fileName in archiveNameList :
                        with archive.open(fileName) as zipin :
                            bufout.write(zipin.read())

                        bufout.seek(0)
                        with open(os.path.join(dest,fileName),"wb") as fout :
                            fout.write(bufout.read())
                        #Truncate and rewind the buffer
                        bufout.seek(0)
                        bufout.truncate()
                

########################################################
##MAIN PROGRAM
########################################################
#define the list of years to download

#Download all non-utility
downloadAndExtract(baseURL + "/archive/xls/f906nonutil1989.zip", outPathBase + "\\1989_nonutility")
downloadAndExtract(baseURL + "/archive/xls/f906nonutil1999.zip", outPathBase + "\\1999_nonutility")
downloadAndExtract(baseURL + "/archive/xls/f906nonutil2000.zip", outPathBase + "\\2000_nonutility")

#Download Utility 1970-2000
##yearList = range(1970,2001)
##
##for yr in yearList :
##    if yr <= 1995 :
##        downloadExcel(baseURL + "/utility/f759" + str(yr) + "u.xls", rootFolder + "\\" + str(yr) + "u.xls")
##    else :
##        downloadExcel(baseURL + "/utility/f759" + str(yr) + "mu.xls", rootFolder + "\\" + str(yr) + "um.xls")
##
#2001 - 2007 EIA-920
yearList = range(2001, 2008)
for yr in yearList :
    print("Processing " + str(yr) + "\n")
    downloadAndExtract(baseURL + "/archive/xls/f906920_" + str(yr) + ".zip", outPathBase + "\\" + str(yr))

#2008+ EIA-923
#Since these represent still-being released data, handling is more complex
#I will check the last-modified time of each file on the webserver and compare it
#to the version I have here. I will download the file if its newer and extract the contents
#to a dated folder.
yearList = range(2008,endYear+1)
for yr in yearList :
    print("Processing " + str(yr) + "\n")
    YearPath = os.path.join(outPathBase,str(yr))
    if not os.path.isdir(YearPath) :
        os.mkdir(YearPath)

    #Get the name of the most recent subfolder in the correct year folder
    #You cleverly name folders so they will sort accoridng to date
    #So you just need to take the max
    DirList = [f.name for f in os.scandir(YearPath) if f.is_dir()]
    if len(DirList) > 0 :
        MostRecent = os.path.join(YearPath,max(DirList))
        LastModifiedTime = dt.datetime.fromtimestamp(os.stat(MostRecent).st_ctime)
        print("Local file downloaded at",LastModifiedTime)
        #Construct the HTTP header. Include "If-Modified-Since" with the last modified date
        hdr = {
            'If-Modified-Since' : LastModifiedTime.astimezone(pytz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        }
    else :
        #We don't have a subfolder, so we want to download the file regardless
        hdr = {'If-Modified-Since' : dt.datetime(yr,1,1).astimezone(pytz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT") }

    #Construct an HTTP request
    URLCurrent = baseURL + "/xls/f923_{yr:04d}.zip".format(yr=yr)
    URLArchive = baseURL + "/archive/xls/f923_{yr:04d}.zip".format(yr=yr)

    with io.BytesIO() as buf :
        NewFile = False
        #Attempt to download the file.
        #If the server responds with a 304 then you already have the most recent version of the file
        try :
            for URL in [URLCurrent,URLArchive] :
                req = urllib.request.Request(URL,headers=hdr)
                print("Trying", URL)
                with urllib.request.urlopen(req) as resp :
                    #print("Server responds positively:", resp.getcode())
                    #print("I could obtain data")
                    #Check to see that the server response is a zip file, if not, check the next URL
                    if resp.info()['Content-Type'] == "application/x-zip-compressed" :
                        print("File on server is newer. Downloading")
                        #Get the last modified date, Check to see if its in the header
                        if 'Last-Modified' in resp.info() :
                            LastModifiedTime = dt.datetime.strptime(resp.info()['Last-Modified'],"%a, %d %b %Y %H:%M:%S %Z")
                            print("Server Version Date:", LastModifiedTime)
                        else :
                            #If not in the header, use today
                            print("Server did not provide a version date. Using today")
                            LastModifiedTime = dt.datetime.now()

                        buf.write(resp.read())
                        NewFile = True
                        #We have our data. Terminate the loop
                        break

        except urllib.error.HTTPError as e :
            if e.code == 304 :
                #print("Server responds positively:", e.code)
                print("The page has not been modified since the last download")
            else :
                print("Server responds badly:", e.code)
                #print("This is an unhandled error")
                raise e

        #If we have data to process
        if NewFile :
            print("Extracting")
            #Create an appropriately-named directory
            buf.seek(0)
            with zipfile.ZipFile(buf,"r") as archin :
                for f in archin.namelist() :
                    try :
                        archin.extract(f,YearPath)
                    except zipfile.BadZipFile as e :
                        print("Unable to extract due to malformed zip file. Skipping")
