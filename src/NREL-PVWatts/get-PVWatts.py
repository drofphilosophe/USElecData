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
import gzip
import time

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

        #Create a log of query datetimes
        self.query_log = []
        

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
        #Count the number of queries run within the past hour
        query_count = len([q for q in self.query_log if q >= dt.datetime.now() - dt.timedelta(hours=1)])
        if query_count >= 990 :
            delay = dt.datetime.now() - self.query_log[-990:-989] - dt.timedelta(hours=1)
            print(f"Query limit exceeded. Sleeping {delay.seconds} seconds")
            time.sleep(delay.seconds)
            
        with io.BytesIO() as buf :
            try :
                #Throttle just a little to be nice
                time.sleep(5)
                
                with urllib.request.urlopen(self.request) as resp :
                    buf.write(resp.read())
                buf.seek(0)
                self.response_json = json.load(buf)
                
            #Trap 429 errors which indicate we've exceeded our request quota
            except urllib.error.HTTPError as e :
                if e.code == 429 :
                    print("The server responded that we've exceeded our request quota. Sleeping 1 hour")
                    time.sleep(60*60)
                    with urllib.request.urlopen(self.request) as resp :
                        buf.write(resp.read())
                    buf.seek(0)
                    self.response_json = json.load(buf)
                else :
                    print("Some other HTTP error. The request parameters follow")
                    for k in self.query_params :
                        print(k,":",self.query_params[k])
                    print("")
                    raise e
                
        self.query_log += [dt.datetime.now()]

##Init the object for communicating with PVWatts
P = PVWatts(url=base_URL,api_key=API_key)

##Load the JSON file of solar plant details
with open(os.path.join(outputRoot,"data","intermediate","Solar","solar-plant-details.json"),"r") as f :
    solar_data = json.load(f)

##Load a pre-existing output file
generation_profiles = []
#A set containing oris,generator-id tuples of plants we've already processed
processed_plants = []

tsv_out_path = os.path.join(outputRoot,"data","intermediate","Solar","solar-generation.profiles.txt.gz")
if os.path.isfile(tsv_out_path) :
    print("Reading a previous output file")
    with io.TextIOWrapper(gzip.open(tsv_out_path,"r"),encoding='utf-8') as f :
        rdr = csv.DictReader(f,delimiter='\t')
        for row in rdr :
            generation_profiles += [row]
            genunit_id = (row['orispl.code'],row['eia.generator.id'])
            if genunit_id not in processed_plants :
                processed_plants += [ genunit_id ]

try :    
##Loop through each solar plant
    for plant in solar_data :

        plant_oris = str(plant["orispl.code"])
        plant_genid = plant["eia.generator.id"]

        if (plant_oris,plant_genid) in processed_plants :
            print(f"We already have data for ORIS: {plant_oris} Generator: {plant_genid}. Skipping")
        else :
            print(f"Prossing ORIS: {plant_oris} Generator: {plant_genid}")
            P.query_timeframe('hourly')
            P.query_latitude(plant["plant.latitude"])
            P.query_longitude(plant["plant.longitude"])
            if float(plant["nameplate.capacity.mw"]) > 0 :
                P.query_system_capacity(plant["nameplate.capacity.mw"])
            else :
                P.query_system_capacity(1)
                
            P.query_tilt(plant["tilt.angle"])
            P.query_azimuth(plant["azimuth.angle"])
            P.query_losses(0)
            P.query_use_wf_albedo(True)
            
            if plant["is.thinfilm"] :
                P.query_module_type('Thin Film')
            else :
                P.query_module_type('Premium')

            if plant["has.single.axis.tracking"] :
                P.query_array_type('1-Axis')
            elif plant["has.dual.axis.tracking"] :
                P.query_array_type('2-Axis')
            else :
                P.query_array_type('Fixed - Open Rack')

            P.get_request()
            P.run_query()

            #Loop through each row and write the output
            #Initialize the date. Be sure to choose a non-leap year
            curr_datetime = dt.datetime(2001,1,1,0)

            for r in P.response_json['outputs']['ac'] :
                output_row = {
                    'orispl.code' : plant_oris,
                    'eia.generator.id' : plant_genid,
                    'month' : curr_datetime.month,
                    'day' : curr_datetime.day,
                    'hour' : curr_datetime.hour,
                    'pvwatts.generation' : r
                }
                generation_profiles += [output_row]

                #Increment the datetime one hour
                curr_datetime += dt.timedelta(hours=1)

            processed_plants += [(plant_oris,plant_genid)]

finally :
    #Write an output file
    print("Writing the output file")
    with io.TextIOWrapper(gzip.open(tsv_out_path,"wb"),encoding='utf-8',newline='\n') as f :
        wtr = csv.DictWriter(
            f,
            fieldnames=['orispl.code','eia.generator.id','month','day','hour','pvwatts.generation'],
            delimiter='\t'
            )
        wtr.writeheader()
        
        for r in generation_profiles :
            wtr.writerow(r)
        
    



        
