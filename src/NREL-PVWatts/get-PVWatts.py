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
import os
import datetime as dt
import io
import yaml
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
outPathBase = os.path.join(outputRoot,"data","source","NREL-PVWatts")

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


#URL of the NREL PVWatts API
base_URL = projectConfig["sources"]["NREL-PVWatts"]["URL"]
API_key = projectLocalConfig["auth"]["NREL-API-key"]

#######################
#######################
## Define a class for interacting with the NREL PVWatts API
#######################
#######################
class PVWatts :

    def __init__(self,url=None,api_key=None,api_version=8) :

        #This class only supports JSON responses
        self.response_format = "json"

        #Init class parameters
        self.set_url(url)
        self.set_api_key(api_key)
        self.set_api_version(api_version)

        #Init a dictionary that will hold query parameters in name : value
        #format
        self.query_params = {}
        

    def set_url(self,url) :
        self.url = url

    def set_api_key(self,k) :
        self.api_key = k

    def set_api_version(self,v) :
        self.api_version = int(v)

    ##################
    ## Query setters
    ##################
    #Each query setter checks the query_params dict
    #for the appropriate key and adds/updates/removes it
    #based on the supplied argument. Setting the argument to None
    #Removes that parameter from the dict (uses the APIs default value)
    def _set_query_float(self,param,x) :
        if x is None :
            if param in self.query_params :
                del self.query_params[param]
        elif param in self.query_params :
            self.query_params[param] = float(x)
        else :
            self.query_params.update( {param : float(x) }

    def query_system_capacity(self,x) :
        self._set_query_float('system_capacity',x)

    def query_module_type(self,x) :
        
            
            
        







        
