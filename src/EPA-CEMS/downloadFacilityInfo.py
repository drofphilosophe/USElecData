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
import http.cookiejar

URL1 = "https://ampd.epa.gov/ampd/#?bookmark=22021"

cookiejar = http.cookiejar.CookieJar()
cookieproc = urllib.request.HTTPCookieProcessor(cookiejar)
opener = urllib.request.build_opener(cookieproc)

with opener.open(URL1) as resp :

    for cookie in cookiejar:
        print(cookie.name, cookie.value)   
