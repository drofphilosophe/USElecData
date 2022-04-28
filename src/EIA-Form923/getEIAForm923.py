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
import datetime
import pytz

#Define some parameters
startYear = 1999 #The program doesn't respect startYear. It just won't download files it already has
endYear = datetime.date.today().year
baseURL = "http://www.eia.gov/electricity/data/eia923/xls"
rootFolder = r"I:\Personal Files\archs\GoogleDrive\Data\Energy\Electricity\EIA\Form EIA-923\data\source"

##################
## You shouldn't need to change anything below this line
##################




###########################
## downloadFile - Downloads an Excel File and saves to specified folder
###########################
def downloadExcel(url, dest) :
    if os.path.exists(dest) :
        print("File exists. Skipping.\n")
    else :
        print("\tI ask server for: " + url)
        response = urllib.request.urlopen(url)
        print("\tServer responds: " + response.headers['content-type'] )

        if response.headers['content-type'] == "application/vnd.ms-excel" :
            #Save Excel file
            print("\tSaving Excel File")

            #Dump the serer response to an appropriately-named binary file
            XLFile = open(dest, 'wb')
            XLFile.write(response.read())
            XLFile.close()

            #Clean up
            response.close()

        else :
            print("No suitable file type found.")

#####################################
# downloadAndExtract - Downloads a ZIP archive and extracts it to the specified folder
#####################################
def downloadAndExtract(url, dest) : #Download and extract ZIP archive
    #Check if the target folder exists. If so, skip this download
    if os.path.exists(dest) :
         print("Folder already exists. Skipping.\n")
    else :

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

        #Some archives contain a subfolder with the year as the name.
        #Move everything out of that subfolder and delete it
        #if os.path.exists(destFolder + "\\" + str(yr)) :
        #    print("\tUpleveling directory " + str(yr))
        #    moveList = os.listdir(destFolder + "\\" + str(yr))
        #    for moveFile in moveList :
        #        os.rename(destFolder + "\\" + str(yr) + "\\" + moveFile, destFolder + "\\" + moveFile)
        #    os.rmdir(destFolder + "\\" + str(yr))

        #Clean up
        archive.close()
        zipFile.close()
        response.close()

########################################################
##MAIN PROGRAM
########################################################
#define the list of years to download

#Download all non-utility
downloadAndExtract(baseURL + "/f906nonutil1989.zip", rootFolder + "\\1989_nonutility")
downloadAndExtract(baseURL + "/f906nonutil1999.zip", rootFolder + "\\1999_nonutility")
downloadAndExtract(baseURL + "/f906nonutil2000.zip", rootFolder + "\\2000_nonutility")

#Download Utility 1970-2000
##yearList = range(1970,2001)
##
##for yr in yearList :
##    if yr <= 1995 :
##        downloadExcel(baseURL + "/utility/f759" + str(yr) + "u.xls", rootFolder + "\\" + str(yr) + "u.xls")
##    else :
##        downloadExcel(baseURL + "/utility/f759" + str(yr) + "mu.xls", rootFolder + "\\" + str(yr) + "um.xls")
##
###2001 - 2007 EIA-920
##yearList = range(2001, 2008)
##for yr in yearList :
##    print("Processing " + str(yr) + "\n")
##    downloadAndExtract(baseURL + "/f906920_" + str(yr) + ".zip", rootFolder + "\\" + str(yr))

#2008+ EIA-923
#Since these represent still-being released data, handling is more complex
#I will check the last-modified time of each file on the webserver and compare it
#to the version I have here. I will download the file if its newer and extract the contents
#to a dated folder.
yearList = range(2008,endYear+1)
for yr in yearList :
    print("Processing " + str(yr) + "\n")
    YearPath = os.path.join(rootFolder,str(yr))
    if not os.path.isdir(YearPath) :
        os.mkdir(YearPath)

    #Get the name of the most recent subfolder in the correct year folder
    #You cleverly name folders so they will sort accoridng to date
    #So you just need to take the max
    DirList = [f.name for f in os.scandir(YearPath) if f.is_dir()]
    if len(DirList) > 0 :
        MostRecent = os.path.join(YearPath,max(DirList))
        LastModifiedTime = datetime.datetime.fromtimestamp(os.stat(MostRecent).st_ctime)
        print("Local file downloaded at",LastModifiedTime)
        #Construct the HTTP header. Include "If-Modified-Since" with the last modified date
        hdr = {
            'If-Modified-Since' : LastModifiedTime.astimezone(pytz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        }
    else :
        #We don't have a subfolder, so we want to download the file regardless
        hdr = {'If-Modified-Since' : datetime.datetime(yr,1,1).astimezone(pytz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT") }

    #Construct an HTTP request
    URLCurrent = "https://www.eia.gov/electricity/data/eia923/xls/f923_{yr:04d}.zip".format(yr=yr)
    URLArchive = "https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{yr:04d}.zip".format(yr=yr)

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
                            LastModifiedTime = datetime.datetime.strptime(resp.info()['Last-Modified'],"%a, %d %b %Y %H:%M:%S %Z")
                            print("Server Version Date:", LastModifiedTime)
                        else :
                            #If not in the header, use today
                            print("Server did not provide a version date. Using today")
                            LastModifiedTime = datetime.datetime.now()

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
            FileVersion = LastModifiedTime.strftime("%Y%m%d")
            OutPath = os.path.join(YearPath,FileVersion)
            os.mkdir(OutPath)
            buf.seek(0)
            with zipfile.ZipFile(buf,"r") as archin :
                for f in archin.namelist() :
                    archin.extract(f,OutPath)
