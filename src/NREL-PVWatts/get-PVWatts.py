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
import urllib.parse
import os
import datetime as dt
import io
import yaml
import csv
import json

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

    MODULE_TYPES = {
        'Standard' : 0,
        'Premium' : 1,
        'Thin Film' : 2
        }

    ARRAY_TYPES = {
        'Fixed - Open Rack' : 0,
        'Fixed - Roof Mounted' : 1,
        '1-Axis' : 2,
        '1-Axis Backtracking' : 3,
        '2-Axis' : 4
    }

    DATASET_NAMES = ['nsrdb','tmy2','tmy3','intl']

    TIMEFRAMES = ['monthly','hourly']
    
    def __init__(self,url=None,api_key=None,api_version=8) :

        #This class only supports JSON responses
        self.response_format = "json"

        #Request headers in a dictionary. We include an
        #empty reference to the API key by default
        self.request_headers = {
            'X-Api-Key' : ''
            }
        
        #Init class parameters
        if url is not None :
            self.set_url(url)
        if api_key is not None :
            self.set_api_key(api_key)
            
        self.set_api_version(api_version)

        #Init a dictionary that will hold query parameters in name : value
        #format
        self.query_params = {}

        #Initialize other class data
        self.query_url = None
        self.request = None
        self.response_json = None
        

    def set_url(self,url) :
        self.url = url

    def set_api_key(self,k) :
        self.api_key = k
        self.request_headers['X-Api-Key'] = k

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
            self.query_params.update( {param : float(x) } )

    def _set_query_int(self,param,x) :
        if x is None :
            if param in self.query_params :
                del self.query_params[param]
        elif param in self.query_params :
            self.query_params[param] = int(x)
        else :
            self.query_params.update( {param : int(x) } )

    def _set_query_str(self,param,x) :
        if x is None :
            if param in self.query_params :
                del self.query_params[param]
        elif param in self.query_params :
            self.query_params[param] = str(x)
        else :
            self.query_params.update( {param : str(x) } )                                
                                      
    def query_system_capacity(self,x) :
        self._set_query_float('system_capacity',x)

    def query_module_type(self,x) :
        if type(x) is str :
            x = self.MODULE_TYPES[x]
            
        self._set_query_int('module_type',x)

    def query_losses(self,x) :
        self._set_query_float('losses',x)

    def query_array_type(self,x) :
        if type(x) is str :
            x = self.ARRAY_TYPES[x]

        self._set_query_int('array_type',x)

    def query_tilt(self,x) :
        self._set_query_float('tilt',x)

    def query_azimuth(self,x) :
        self._set_query_float('azimuth',x)

    def query_address(self,x) :
        raise ValueError("Addresses not implemented")

    def query_latitude(self,x) :
        self._set_query_float('lat',x)

    def query_longitude(self,x) :
        self._set_query_float('lon',x)

    def query_dataset(self,x) :
        if x not in self.DATASET_NAMES :
            raise ValueError("Unknown dataset name")
        self._set_query_str('dataset',x)

    def query_radius(self,x) :
        self._set_query_int('radius',x)

    def query_timeframe(self,x) :
        if x not in self.TIMEFRAMES :
            raise ValueError("Unknown timeframe")
        self._set_query_str('timeframe',x)
            
    def query_dc_ac_ratio(self,x) :
        self._set_query_float('dc_ac_ratio',x)

    def query_gcr(self,x) :
        self._set_query_float('gcr',x)

    def query_inv_eff(self,x) :
        self._set_query_float('inv_eff',x)

    def query_bifaciality(self,x) :
        self._set_query_float('bifaciality',x)

    def query_albedo(self,x) :
        self._set_query_float('albedo',x)

    def query_use_wf_albedo(self,x) :
        if type(x) is bool :
            x = int(x)
        self._set_query_int('use_wf_albedo',x)

    def query_soiling(self,x) :
        raise ValueError("Soiling not implemented")


    ###########################
    ## Query Methods
    ###########################
    #Construct a request object
    def get_request(self) :
        #Contruct the URL
        self.query_url = f"{self.url}/api/pvwatts/v{self.api_version}.{self.response_format}?" + \
                    urllib.parse.urlencode(self.query_params)
        
        self.request = urllib.request.Request(self.query_url,headers=self.request_headers)


    def run_query(self) :
        with io.BytesIO() as buf :
            with urllib.request.urlopen(self.request) as resp :
                buf.write(resp.read())
            buf.seek(0)
            self.response_json = json.load(buf)
            


P = PVWatts(url=base_URL,api_key=API_key)
P.query_timeframe('hourly')
P.query_array_type('Fixed - Open Rack')
P.query_module_type('Premium')
P.query_losses(0)
P.query_latitude(39.98587)
P.query_longitude(-76.94105)
P.query_system_capacity(1000)
P.query_tilt(20)
P.query_azimuth(165)
P.get_request()
P.run_query()
print(P.response_json.keys())
#print(P.query_url + f"&api_key={P.api_key}")


        
