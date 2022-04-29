######################
## downloadFacilityInfo.py
##
## Download facility attributes from the AMPD website
## This code is currently non-functional and the file
## should be downloaded manually
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
