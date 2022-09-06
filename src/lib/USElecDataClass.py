##########################
## Define a class for common USElecData in Python
##########################
import yaml
import os
import sys

class USElecDataClass :

    #Name of the Anaconda environment in which all scripts should be run
    CONDA_ENV_NAME = "USElecData"

    def __init__(self,warnings=True,ignore_environment=False) :

        self.warnings = warnings
        self.projectRoot = None         #Root of project code and config files
        self.outputRoot = None          #Root of project data
        self.inCondaEnv = None          #True if we are operating in a USElecData conda environment
        self.envVarsSet = None          #True if the USElecData environment variables are set
        self.localConfigDirty = None    #True if the local configuration has changed since it was loaded. None until it is loaded. Then False. 
        self.config_local_path = None
        self.config_path = None
        self.localConfig = None
        self.globalConfig = None
        
        ##########################
        ## Display the banner
        ##########################
        if self.warnings :
            self.print_banner()

        #Determine if we're running in the USEledData conda environment
        #Exit with an exception if this is not what the user intended
        if os.getenv('CONDA_DEFAULT_ENV') == "USElecData" :
            self.inCondaEnv = True
        elif ignore_environment :
            self.inCondaEnv = True
            if self.warnings :
                print(
                    "----- WARNING -----",
                    "It appears you are not running this script in the USElecData Anaconda environment",
                    "but you have instructed the script to ignore it.",
                    "",
                    sep='\n'
                )
        else :
            self.inCondaEnv = False
            if self.warnings :
                print(
                    "----- WARNING ----",
                    f"It appears you are not running this script in the {self.CONDA_ENV_NAME} Anaconda environment.",
                    "This code package has only been tested against that enviroment and may behave unexpectedly,",
                    "fail, or fail to replicate if executed in other environments.",
                    "",
                    "You should create a new Anaconda environment using",
                    ""," > conda env create -f USElecData_conda.yaml","",
                    "Then activate the enviroment using",
                    "",f" > conda activate {self.CONDA_ENV_NAME}","",
                    "If you choose to run scripts they may attempt to make permenant changes to the current environment",
                    "such as changing environment variables or installing R packages.",
                    f"Be aware these changes will not be isolated to the {self.CONDA_ENV_NAME} Anaconda environment",
                    sep="\n"
                )

                resp = input("Are you sure you want to continue? [y/N] ")

                if resp.lower() != "y" :
                    print("Exiting")
                    raise Exception("Attempted to execute outside of USElecData Anaconda Environment")
                    
                self.inCondaEnv = True
                print(
                    f"Further checks that you are running in the {self.CONDA_ENV_NAME} Anaconda environment have been disabled"
                )
                
            
        #Determine the project root and output root
        #These should be stored in environment variables
        #If not, we'll look in the current directory and
        #work our way up to the root looking for the config_local.yaml file
        self.projectRoot = os.getenv('USELECDATA_PROJECTROOT')
        self.outputRoot = os.getenv('USELECDATA_OUTPUTROOT')
        
        if self.projectRoot is None or self.outputRoot is None:
            self.envVarsSet = False
            if self.warnings:
                print("WARNING: Environment variables for this project are not set.")
                print("You should run the initialization script at")
                print("\n > python src/USElecData.py init --output-path=<output-data-path>\n\n")
                
            check_path, _ = os.path.split(os.path.realpath(__file__))
            self.config_path = None
            counter = 0
            while os.path.exists(check_path) and self.config_path is None :
                
                if os.path.isfile(os.path.join(check_path,"config.yaml")) :
                    self.config_path = os.path.join(check_path,"config.yaml")
                    #Load the configuration files
                    self.loadGlobalConfig()
                    if os.path.isfile(os.path.join(check_path,"config_local.yaml")) :
                        self.config_local_path = os.path.join(check_path,"config_local.yaml")
                        self.loadLocalConfig()
                    else :
                        if warnings :
                            print(
                                "Global configuration file found with no corrisponding local configuration file.",
                                "Run 'USElecData init' to initalize the local configuration file."
                            )
                        self.projectRoot = self.config_path
                else :
                    #Move up one directory and try again
                    check_path = os.path.abspath(os.path.join(check_path,".."))
                    counter+=1
                    if counter > 10 :
                        raise Exception("Environment not set and unable to find configuration file. Aborting.")

            if warnings :
                print("I found a configuration file at\n",self.config_path)

            self.config_local_path = os.path.join(check_path,"config_local.yaml")


            if self.projectRoot is None or self.outputRoot is None :
                raise Exception("Cannot determine the path to project files. You should run the configuration script")
        else :
            self.envVarsSet = True
            self.config_local_path = os.path.join(self.projectRoot,"config_local.yaml")
            self.config_path = os.path.join(self.projectRoot,"config.yaml")
            self.loadLocalConfig()
            self.loadGlobalConfig()

            
    ###############################
    ###############################
    ## Load configuration information from files
    ###############################
    ###############################
    def loadLocalConfig(self,suppress_warnings=False) :
        #The local configuration file exists
        if self.config_local_path is not None and os.path.isfile(self.config_local_path) :
            
            with open(self.config_local_path) as yamlin :
                self.localConfig = yaml.safe_load(yamlin)

            if self.projectRoot is None :
                self.projectRoot, _ = os.path.split(self.config_local_path)

            if self.outputRoot is None :
                self.outputRoot = self.localConfig["output"]["path"]

            self.localConfigDirty = False
        else :
            if suppress_warnings == False and self.warnings :
                print("WARNING: The local configuration file does not exist.")
                print("As of this version you need to create it manually.")

        #########
        ## Fill in missing information
        #########
        if self.localConfig is None :
            self.localConfig = {}
            self.localConfigDirty = True
            
        if "output" not in self.localConfig.keys() :
            self.localConfig.update({"output" : {}})
            self.localConfigDirty = True

        if "path" not in self.localConfig["output"].keys() :
            self.localConfig["output"].update({"path" : ""})
            self.localConfigDirty = True

        if "formats" not in self.localConfig["output"].keys() :
            self.localConfig["output"].update({"formats" : []})
            self.localConfigDirty = True

        if "auth" not in self.localConfig.keys() :
            self.localConfig.update({'auth' : {} } )
            self.localConfigDirty = True

        if "EPA-CAMPD-API-key" not in self.localConfig["auth"].keys() :
            self.localConfig["auth"].update({'EPA-CAMPD-API-key' : ''})
            self.localConfigDirty = True
                                    
                
    def loadGlobalConfig(self) :
        #The local configuration file exists
        if os.path.isfile(self.config_path) :
            
            with open(self.config_path) as yamlin :
                self.globalConfig = yaml.safe_load(yamlin)

        else :
            if self.warnings :
                print("WARNING: The global configuration file does not exist.")

    ############################
    ## Adjust components of the local configuration
    ## each method is setlocal_*
    ############################
    def setlocal_output_path(self,path) :
        #Check to see if the path exists
        if os.path.isdir(path) :
            self.localConfig["output"]["path"] = os.path.abspath(path)
            self.localConfigDirty = True

    def setlocal_apikey_campd(self,keytext) :
        self.localConfig["auth"]['EPA-CAMPD-API-key'] = keytext

    def getlocal_apikey_campd(self) :
        return self.localConfig["auth"]['EPA-CAMPD-API-key']

    ########################
    ## Write a local configuration file
    ########################
    def write_local_config(self) :
        if self.localConfigDirty == False :
            if os.access(self.config_local_path,os.W_OK) :
                with open(self.config_local_path,"w") as yamlout :
                    yaml.dump(self.localConfig,yamlout)
            else :
                raise FileNotFoundError("Local configuration file location not set or is not writable")


        
    ###############################
    ###############################
    ## Set configuration formation in enviroment variables
    ###############################
    ###############################
    def setEnvironmentVariables(self) :
        if self.projectRoot is None :
            raise Exception("Cannot set environment variables because the script was unable to determine the project root path.")
        elif self.outputRoot is None :
            raise Exception("Cannot set environment variables because the script was unable to determine the output data root path.")
        elif self.inCondaEnv == False :
            raise Exception(f"Cannot set environment variables because the script is not running in the {self.CONDA_ENV_NAME} Anaconda environment.")
        elif self.envVarsSet :
            raise Exception("Cannot set environment variables because they are already set. Delete them first.")
        else :
            cmdText = 'conda env config vars set {varname:s}="{value:s}"'
            os.system(cmdText.format(varname="USELECDATA_PROJECTROOT",value=self.projectRoot))
            os.system(cmdText.format(varname="USELECDATA_OUTPUTROOT",value=self.outputRoot))
            print("Environment variables reset. You need to reactivate your Anaconda environment")
            self.envVarsSet = True

    def unsetEnvironmentVariables(self) :
        if self.inCondaEnv == False :
            raise Exception(f"Cannot set environment variables because the script is not running in the {self.CONDA_ENV_NAME} Anaconda environment.")
        elif self.envVarsSet == False :
            raise Exception("Cannot set environment variables because they are not currently set.")
        else :
            cmdText = 'conda env config vars unset {varname:s}'
            os.system(cmdText.format(varname="USELECDATA_PROJECTROOT"))
            os.system(cmdText.format(varname="USELECDATA_OUTPUTROOT"))
            self.envVarsSet = False


    def addLibraryPath(self) :
        print("Creating conda.pth for this project")
        path_cmd_text = 'conda env config vars set PYTHONPATH="{fullpath:s}"'
        os.system(path_cmd_text.format(fullpath=os.path.join(self.projectRoot,"src","lib")))

    def removeLibraryPath(self) :
        print("Removing conda.pth for this project")
        path_cmd_text = 'conda env config vars unset PYTHONPATH'
        os.system(path_cmd_text)

        
    ##############################################################
    ##############################################################
    ##############################################################
    ## High-level Management Functions
    ##############################################################
    ##############################################################
    ##############################################################


    ############################
    ## Project Information
    ############################
    def print_banner(self) :
        print(
                "",
                "┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐",
                "│  USElecData                                                                                                 │",
                "│                                                                                                             │",
                "│ High-frequency data on electricity generation, consumption, and emissions in the United States.             │",
                "│ All software in this repository is licensed under the Apache License, Version 2.0.                          │",
                "│ See the LICENSE file or run 'USElecData license' to view the license.                                       │",
                "│ Use of these data requires citation. Use 'USElecData citation' for more information                         │",
                "└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘",
                "",
                sep='\n'
            )
        
    def print_license(self) :
        #Display the license text
        with open(os.path.join(self.projectRoot,"LICENSE"),'r') as fin :
            print(fin.read())

    def print_citation(self) :
        print("Archsmith, J. E. (2022). US Electricity Data Pratictioners Guide")

    def print_bibtex_citation(self) :
        print(
            "@misc{Archsmith_US_Electricity_Data_2022,",
            "author = {Archsmith, James E.},",
            "title = {{US Electricity Data Pratictioners Guide}},",
            "year = {2022}",
            "}",
            sep='\n'
            )


    ########################
    ## Project environment initalization
    ## This will commit the local configuration file
    ## and set environment variables in the current Anaconda environment
    ########################
    def init_environment(self) :
        self.write_local_config()

        self.run_r_script(os.path.join("src","init","initialize_R_environment.R"))
                          
        if self.envVarsSet == False :
            self.setEnvironmentVariables()

        

    #################################
    ## Handlers for running script files
    #################################
    def run_python_script(self,scriptpath) :
        fullscriptpath = os.path.abspath(os.path.join(self.projectRoot,scriptpath))
        if os.path.isfile(scriptpath) :
            try :
                print(f"\n----- Running Python script {scriptpath} -----")
                os.system(f'python "{fullscriptpath}"')
                print(f"\n----- End Python script {scriptpath} -----")
                retval = 0
            except Exception as e :
                print(e)
                retval = -1
                sys.exit(-1)
        else :
            print(f"Python script not found {scriptpath}")
            retval = -1
            sys.exit(-1)
            
        return retval

    def run_r_script(self,scriptpath) :
        fullscriptpath = os.path.abspath(os.path.join(self.projectRoot,scriptpath))
        if os.path.isfile(fullscriptpath) :
            try :
                print(f"\n----- Running R script {scriptpath} -----")
                os.system(f'Rscript "{fullscriptpath}"')
                print(f"----- End R script {scriptpath} -----\n")
                retval = 0
            except Exception as e :
                print(e)
                retval = -1
                sys.exit(-1)
        else :
            print(f"R script not found {fullscriptpath}")
            retval = -1
            sys.exit(-1)
            
        return retval
    
if __name__ == "__main__" :
    usd = USElecDataClass()

