import urllib.request
from bs4 import BeautifulSoup
import csv
import gzip
import io
import os.path
import time
import tarfile
import datetime as dt
import socket
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
outPathBase = os.path.join(outputRoot,"data","source","NRC-RPSR")

#Construct the directory tree if it doesn't already exist
if os.path.isdir(outputRoot) :
    #Check that the data directiry exists
    if not os.path.isdir(outPathBase) :
        os.makedirs(outPathBase)
        
    #Create output subfolders if needed
    for d in ['raw-html','processed-csv'] :
        if not os.path.isdir(os.path.join(outPathBase,d)) :
            os.mkdir(os.path.join(outPathBase,d))
else :
    raise FileNotFoundError(
    "The data output defined path in the configuration file does not exist\n" +
    "Expecing path: " + outputRoot
    )




#Path to EPA hourly CEMS data. Subfolders should be years
base_URL = projectConfig["sources"]["NRC-RPSR"]["URL-base"]

#Determine the first date to process
start_date = projectConfig["sources"]["NRC-RPSR"]["start-date"]

#read the end date as a string because we'll need to do some work on it
end_date = projectConfig["sources"]["NRC-RPSR"]["end-date"]

#Missing dates default to yesterday
if end_date is None or end_date.strip() == "" :
    end_date = dt.date.today() - dt.timedelta(days=1)
elif type(end_date) == dt.date :
    pass
else :
    end_date = dt.datetime.strptime(end_date,"%Y-%m-%d").date()




#Define custom headers to transmit
#I get an absurd number of timeouts
#Wondering if they throttle the python http client

class RPSR :
    #A formatted string with the URL of each page
    URL_pattern = "{baseURL}/{year:4.0f}/{year:04d}{month:02d}{day:02d}ps.html"

    #Formatted string for the name of gzipped TARs of the downloaded HTML pages
    rawArchivePattern = "ReactorPowerStatus_Raw_{year:04d}.tar.gz"

    #Formatted string for the name of an individual HTML file within a raw archive
    filenamePattern = "ReactorPowerStats_Raw_{d.year:04d}{d.month:02d}{d.day:02d}.html"

    #Formatted string for the name of a processed TSV of data
    processedArchivePattern = "ReactorPowerStatus_{year:04d}.txt.gz"

    #HTTP headerst to advertise in a request
    headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
        }

    #HTTP timeout in seconds
    httpTimeout = 30
    #Max Number of failed HTTP requests
    failMax = 10
    #Delay after a failed HTTP request
    failDelay = 30
    #Delay between HTTP requests
    httpDelay = 5

    #Debug level
    DEBUG = 5

    #A list of datetime.date values for which no information are available
    BAD_DATES = [
        dt.date(1999,5,8)
        ]

    
    def __init__(self) :

        #The current year of data we're working on
        self.year = None
        
        #Path to data folder
        self.dataPath = None

        #Base URL of reports
        self.base_URL = None
        
        #A dictionary where the key is a date
        #and the value is the content of an HTML file
        self.fileDict = {}
        self.fileDictDirty = False

        #Parsed data. This will be a list of dictionaries
        #That can be written with csv.DictWriter
        self.parsedData = []
        
        #Column names in parsed ouptput data. For now just the date, which we always include.
        #We'll update it as we read in data
        self.parsedColNames = set(["Date"])

    def setBaseURL(self,url) :
        self.base_URL = url
        
    def setDataPath(self,path) :
        if os.path.isdir(path) :
            self.dataPath = path
        else :
            print("The specified data folder does not exist")
            raise FileNotFoundException

    def setYear(self,yr) :
        self.year = yr

        
    def readArchive(self) :
        #Flush the file dictionary
        self.fileDict = {}
        self.fileDictDirty = True

        #If the archive exists, open it and read each file into the dict
        archivePath = os.path.join(self.dataPath,"raw-html",self.rawArchivePattern.format(year=self.year))
        
        if os.path.isfile(archivePath) :
            print("Loading file",archivePath)
            with gzip.open(archivePath,mode="r") as gzfile :
                with tarfile.TarFile(fileobj=gzfile,mode='r') as tf :
                    #Iterate over members. f will be a TarInfo object
                    for f in tf.getmembers() :
                        #Extract the date from the filename
                        #Files are named "ReactorPowerStats_Raw_YYYYMMDD.html"
                        filedate = dt.datetime.strptime(f.name[22:30],"%Y%m%d").date()
                        #Put the date and file data in the dictionary
                        self.fileDict.update( {filedate : tf.extractfile(f).read()} )

            self.fileDictDirty = False
        else :
            print("File",archivePath,"does not exist")
                
    def writeArchive(self) :
        archivePath = os.path.join(self.dataPath,"raw-html",self.rawArchivePattern.format(year=self.year))
        
        if self.fileDictDirty :
            print("Writing archive",archivePath)
            #Write each member to a tar archive and flush that to the disk
            with io.BytesIO() as buf :
                with tarfile.TarFile(fileobj=buf,mode="w") as tf :
                    #Iterate over the file dictionary
                    for filedate in self.fileDict :
                        ti = tarfile.TarInfo(self.filenamePattern.format(d=filedate))
                        ti.size = len(self.fileDict[filedate])
                        tf.addfile(ti,fileobj=io.BytesIO(self.fileDict[filedate]))

                #Rewind the buffer
                buf.seek(0)
                #Write it to an output tar.gz file
                with gzip.open(archivePath,mode="w") as gzfile :
                    gzfile.write(buf.read())
        else :
            print("No changes to",archivePath)

    #Get a report for a given date and add it to the fileDict
    #Returns:
    # 0 - Success
    # 1 - Request timed out
    # 2 - HTTP Error 404
    # 3 - Other HTTP error
    # 4 - Other Error
    
    def getReport(self,d) :
        URL = self.URL_pattern.format(baseURL=self.base_URL,year=d.year,month=d.month,day=d.day)

        #Build a request
        req = urllib.request.Request(URL,headers=self.headers)

        #Set the status
        status = -1
        
        #Read buffer for the page
        with io.BytesIO() as buf :
            #Attempt to download the page
            try :
                with urllib.request.urlopen(req, timeout=self.httpTimeout) as response :
                    buf.write(response.read())
                    status = 0
                    
            except urllib.error.HTTPError as e :
                if e.code == 404 :
                    if self.DEBUG > 0 :
                        print("\tFile does not exist. Skipping")
                    status = 2
                else :
                    if self.DEBUG > 0 :
                        print("\tDownload failed. Error code:", e.code)
                    status = 3
            except urllib.error.URLError as e:
                if self.DEBUG > 0 :
                    print("\tURL Error. Likley a timeout")
                status = 1
            except socket.timeout as e :
                if self.DEBUG > 0 :
                    print("\tSocket Timeout.")
                status = 1
            except Exception as e :
                if self.DEBUG > 0 :
                    print("\tOtherException")
                    print(str(e))
                status = 4
                #raise e

            #If we downloaded the page, add it to the fileDict
            if status == 0 :
                buf.seek(0)
                self.fileDictDirty = True
                self.fileDict.update( {d : buf.read()} )

        return status

    #Check to see if we already have a report
    #Return True if we do, False if we don't
    def checkReport(self,d) :
        status = False
        if d in self.fileDict :
            if len(self.fileDict[d]) > 0 :
                status = True
        return status


    #Try to obtain a report. We'll keep trying until a fail condition is met
    def tryReport(self,d) :
        if self.checkReport(d) :
            if self.DEBUG > 2 :
                print("We already have a report from",d)
        elif d in self.BAD_DATES :
            if self.DEBUG > 2 :
                print("Date",d,"is in the list of dates for which there are no data")
        else :
            print("Attempting to download report from",d,end='')
            failCount = 0
            status = -1
            while failCount < self.failMax :
                time.sleep(self.httpDelay)
                status = self.getReport(d)
                if status == 0 :
                    print(".",end='')
                    break
                elif status == 1 :
                    print("o",end='')
                    #HTTP errors. Let's try again
                    time.sleep(self.failDelay)
                    failCount+=1
                elif status == 2 :
                    #A 404 error
                    print("-",end='')
                    failCount = self.failMax
                elif status < 4 :
                    print("x",end='')
                    #HTTP errors. Let's try again
                    time.sleep(self.failDelay)
                    failCount+=1
                else :
                    print("X",end='')
                    #Critical fail. Abort
                    failCount = self.failMax
            if status == 0 :
                print(" Success")
            else :
                print(" Fail")

    ######################
    ## Stage 2 - Parsing
    ######################
    #Open a report in the archive and parse out
    #the operations information
    def parseReport(self,d) :
        print("Parsing raw data for",d,end=' ')
        #Identify the archive that would contain the report file for day d
        rawArchivePath = os.path.join(self.dataPath,"raw-html",self.rawArchivePattern.format(year=d.year))
        rawFilename = self.filenamePattern.format(d=d)
        
        #Does the archive exist?
        if not os.path.isfile(rawArchivePath) :
            print("The archive for",d,"does not exist")
            print("I expected file",rawArchviePath)
            raise FileNotFoundException

        #Open the archive
        with gzip.open(rawArchivePath,mode="r") as gzfile :
            with tarfile.TarFile(fileobj=gzfile,mode='r') as tf :
                #Init storage of the records we'll read from the file
                records = None
                #Check to be sure we have the raw file in the archive
                if not rawFilename in tf.getnames() :
                    print("The raw HTML file for",d,"does not exist")
                    print("I expected file",rawFilename,"in",rawArchivePath)
                    #Some files just don't exist so don't break the script if you can't find a file
                    #raise FileNotFoundError
                #Read the report and parse it. This should return a list of dictionaries
                #That we will append to the current list
                else :
                    records = self.parseReportWorker(tf.extractfile(rawFilename),d)

                #On an error the worker returns None
                if records :
                    self.parsedData.extend(records)
                    print("Read",len(records),"records")
                else :
                    print("Unable to parse data")


    #read the supplied binary buffer, and parse the HTML within
    def parseReportWorker(self,buf,d) :
        #Init storage for the data we're going to parse
        allData = []

        #Init a BS parser
        soup = BeautifulSoup( buf.read().decode("utf-8", errors='ignore'), features="html.parser" )
        
        #Loop through the tables in this document
        tableList = soup.find_all("table")
        for t in tableList :
            #The tables were interested in have the "summary" attribute set
            if "summary" in t.attrs:
                colNames = []
                #Sometimes the data are immdiately in <tr> records
                #Other times they are nested within <thead> and <tbody> tags
                #Construct a list of all <tr> nested within this table
                rows = t.find_all("tr")
                #for r in rows :
                #    print(r)
                #raise Exception
                #read each table row
                for row in rows :                   
                    dataRow = {}
                    if row.name == "tr" :
                        #Loop through each element
                        colCounter = 0
                        for cell in row.children :
                            #<th> denotes column names
                            cellText = cell.string
                            if cell.name == "th" :
                                if cellText == "Change in report(*)" :
                                    cellText = "Change in report (*)"
                                elif cellText == "Number of Scrams(#)" :
                                    cellText = "Number of Scrams (#)"
                                colNames.append(cellText)
                            if cell.name == "td" :
                                dataRow.update( {colNames[colCounter] : cellText} )
                                colCounter+=1
                    #Add the date as a column
                    if len(dataRow) > 0 :
                        dataRow.update({'Date' : d.strftime("%Y-%m-%d")})
                        allData.append(dataRow)
                #Append the colNames to the allColNames set
                self.parsedColNames = self.parsedColNames.union(set(colNames))

        return allData

    

    #Write a gzipped TSV of the parsed data
    def writeParsedData(self) :
        #Write the TSV to a buffer. gzip will want a binary file
        with io.BytesIO() as buf :
            #Wrap the binary buffer in text
            with io.TextIOWrapper(buf,newline='\n',errors='ignore', encoding="utf-8") as textbuf :
                #Create a dictwriter
                writer = csv.DictWriter(textbuf,fieldnames=self.parsedColNames,restval="",delimiter="\t")
                writer.writeheader()

                rowCounter = 0
                #Write each row
                for row in self.parsedData :
                    writer.writerow(row)
                    rowCounter += 1

                #Explicitly flush the wrapper
                textbuf.flush()
                #Rewind the buffer
                buf.seek(0)

                #Write buffer contents to a data file
                outFilePath = os.path.join(self.dataPath,"processed-csv",self.processedArchivePattern.format(year=self.year))
                with gzip.open(outFilePath, "wb") as gzout :
                    gzout.write(buf.read())

                #Tell us what happened
                print("Wrote file",outFilePath)
                print("With",rowCounter,"rows")
        



#Raw data are aligned annually, so loop through years        
for yr in range(start_date.year,end_date.year + 1) :
    print("Processing",yr)

    P = RPSR()
    P.setBaseURL(base_URL)
    P.setDataPath(outPathBase)
    P.setYear(yr)

    #Read any preexisting archive
    P.readArchive()
    
    #Loop through every day this year or starting with the StartDate
    d = max(start_date,dt.date(yr,1,1))
    #Download the data if we don't have it
    try :
        while d < min(dt.date(yr+1,1,1),end_date) :
            P.tryReport(d)
            d+=dt.timedelta(days=1)
    finally :
        P.writeArchive()

    d = max(start_date,dt.date(yr,1,1))   

    ##Parse the raw HTML into a CSV
    while d < min(dt.date(yr+1,1,1),end_date) :
        P.parseReport(d)
        d+=dt.timedelta(days=1)
    P.writeParsedData()



            
