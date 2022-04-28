#Download and unzip all of EIA Form 860. The program will delete existing folders
#and overwrite them. There are "utility" and "non-utility" files from 1998 to 2000.
#Account for that weirdness as well.
import urllib.request
import urllib.parse
import io
import os
import zipfile
import shutil

#Define some parameters
startYear = 1990
endYear = 2021
baseURL = "http://www.eia.gov/electricity/data/eia860"
googleDrive = os.path.join("G:\\","My Drive")
rootFolder = os.path.join(googleDrive,"Data", "Energy", "Electricity", "EIA","Form EIA-860","data","source")

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
yearList = range(startYear, endYear+1)

for yr in yearList :
    print("Year: " + str(yr))
    destFolder = rootFolder + '\\' + str(yr)


    #Let us know what is happening
    print("\tDownloading " + str(yr) + "....")

    #There are three different formats of files. Treat them differently
    if yr < 1998 :
        downloadAndExtract(baseURL + "/eia860a/eia860a" + str(yr) + ".zip", rootFolder + "\\" + str(yr) + "a")
    elif yr < 2001 :
        downloadAndExtract(baseURL + "/eia860a/eia860a" + str(yr) + ".zip", rootFolder + "\\" + str(yr) + "a")
        downloadAndExtract(baseURL + "/eia860b/eia860b" + str(yr) + ".zip", rootFolder + "\\" + str(yr) + "b")
    else :
        downloadAndExtract(baseURL + "/xls/eia860" + str(yr) + ".zip", rootFolder + "\\" + str(yr))
