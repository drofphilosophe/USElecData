#######################
## USElecData.py
##
## This is the management script for the USElecData project
## it will run all of the R and Python code in this repo
## in the correct order. It can be used to build the initial
## dataset and then update it over time.
##
## It works by making system calls to run python and R scripts
## and assumes it is run in an Anaconda environment defined
## by USElecData_conda.yaml. One should construct this environment
## first with
##
## conda create -f USElecData_conda.yaml
##
## Then activate the environment with
##
## conda activate USElecData
##
## Then run a full build of the data with
##
## python USElecData.py
########################
import argparse
import sys
import os
#Add ./src/lib to the current path. This is where the USElecDataClass.py file lives
#WE may run this script wiht the current working directory as ./ or ./src
sys.path.insert(0, './lib')
sys.path.insert(0,'./src/lib')
import USElecDataClass
import io
import gzip
#import tar
import zipfile


################
## License
################
def processLicense(us_ed,sp_args) :
    print("USElecData Software License\n\n")
    us_ed.print_license()

################
## Citation
################        
def processCitation(us_ed,sp_args) :
    print("USElecData Preferred Citation\n\n")
    #Display the citation text
    if sp_args.bibtex :
        us_ed.print_bibtex_citation()
    else :
        us_ed.print_citation()

################
## Init
################        
def processInit(us_ed,sp_args) :
    print("Initalizing USElecData Local Environment\n\n")
    us_ed.loadLocalConfig(suppress_warnings=True)

    if sp_args.output_path :
        print(f"Setting output path to '{sp_args.output_path}'")
        us_ed.setlocal_output_path(sp_args.output_path)

    if us_ed.getlocal_apikey_campd() == "" :
        print(
            "The local configuration files does not contain a Data.gov key.",
            "You can sign up for a key at https://api.data.gov/signup/",
            sep="\n"
        )
        keytext = input("Enter your Data.gov API here here or press [Enter] to cancel: ")

        if keytext == "" :
            print("No API key entred. Aborting.")
            sys.exit(-1)
        else :
            #Save the API key stripped of any whitespace
            print("Installing required R packages")
            us_ed.setlocal_apikey_campd(keytext.strip())
            
    print("Setting local environment variables")
    us_ed.init_environment()

    #Add the src/lib folder to the Path in this environment
    us_ed.addLibraryPath()

    ##Add src/lib to the path using a .pth file
    

    print(
        "",
        "Local environment initialized. You should reactivate this Anaconda environment",
        "before continuting.",
        "",
        sep="\n"
    )

################
## Source
################        
def processSource(us_ed,sp_args) :
    print(
        "USElecData Processing Source Data",
        "",
        "WARNING: The --resource flag is not currently supported",
        sep="\n"
        )

    ##Create a dictionary of source data product keys and descriptions
    source_dict = {
        'TZInfo' : 'Time zone information',
        'EIA860' : "EIA Form 860 - Electricity Generator Data",
        'EIA923' : "EIA Form 923 - Fuel and Generation for Electricity Consumption",
        'CEMS' : "EPA Continuous Emissions Monitoring System (CEMS)",
        'NRC' : "Nuclear Regulatory Comission Reactor Power Status Reports"
    }

    #Respond with the list of data sources to --list
    if sp_args.list :
        print("\nSoure Data List\n")
        print("Name".ljust(20),"Description")
        for k in source_dict :
            print(k.ljust(20),source_dict[k])

    else :
        print("Updating existing data....")
        #Otherwise download
        if len(sp_args.sources) == 0 or "all" in sp_args.sources :
            sources = source_dict.keys()
        else :
            sources = []
            for s in sp_args.sources :
                if s in source_dict :
                    sources += [s]
                else :
                    print("Unknown source data product",s)
                    print("Use USElecData source --list for a list of source data products")
                    sys.exit(-1)
            
        ################
        ## tz-info
        ################
        if "TZInfo" in sources :
            print(source_dict["TZInfo"])
            us_ed.run_python_script(os.path.join("src","tz-info","getTZInfo.py"))


        ################
        ## EIA Form 860
        ################
        if "EIA860" in sources :
            print(source_dict["EIA860"])
            us_ed.run_python_script(os.path.join("src","EIA-Form860","getEIAForm860.py"))


        ################
        ## EIA Form 923
        ################
        if "EIA923" in sources :
            print(source_dict["EIA923"])
            us_ed.run_python_script(os.path.join("src","EIA-Form923","getEIAForm923.py"))

        ################
        ## EIA Form 923
        ################
        if "CEMS" in sources :
            print(source_dict["CEMS"])
            us_ed.run_python_script(os.path.join("src","EPA-CEMS","downloadCEMSData.py"))
            us_ed.run_python_script(os.path.join("src","EPA-CEMS","downloadFacilityInfo.py"))

        ###############
        ## NRC RPSR
        ###############
        if "NRC" in sources :
            print(source_dict["NRC"])
            us_ed.run_python_script(os.path.join("src","NRC-RPSR","getReactorPowerStatusReports.py"))
            
        print("Data download complete")
    

def processBuild(us_ed,sp_args) :
    print(
        "USElecData Building Data",
        "",
        "WARNING: The --rebuild flag is not currently supported",
        sep="\n"
        )

    ##Create a dictionary of source data product keys and descriptions
    product_dict = {
        'TZInfo' : 'Time zone information',
        'EIA860' : "EIA Form 860 - Electricity Generator Data",
        'EIA923' : "EIA Form 923 - Fuel and Generation for Electricity Consumption",
        'CEMS' : "EPA Continuous Emissions Monitoring System (CEMS)",
        'CEMS_Facility' : 'CEMS Facility Data',
        'CEMS_Operations' : 'CEMS Operations Data',
        'Crosswalks' : 'Crosswalks between datasets',
        'NRC' : "Nuclear Regulatory Comission Reactor Power Status Reports"
    }

    #Respond with the list of data sources to --list
    if sp_args.list :
        print("\nDatastore list\n")
        print("Name".ljust(20),"Description")
        for k in product_dict :
            print(k.ljust(20),product_dict[k])

    else :
        print("Updating existing data products....")
        #Otherwise download
        if len(sp_args.products) == 0 or "all" in sp_args.products :
            products = product_dict.keys()
        else :
            products = []
            for p in sp_args.products :
                if p in product_dict :
                    products += [p]
                else :
                    print("Unknown data product",p)
                    print("Use USElecData build --list to see a list of data products")
                    sys.exit(-1)
                    
        ################
        ## tz-info
        ################
        if "TZInfo" in products :
            print(product_dict["TZInfo"])
            print("No build required")
        
        ################
        ## EIA Form 860
        ################
        if "EIA860" in products :
            print(product_dict["EIA860"])            
            us_ed.run_r_script(os.path.join("src","EIA-Form860","loadEIA860_Schedule3_Generators.R"))
            us_ed.run_r_script(os.path.join("src","EIA-Form860","loadEIA860_Schedule6.R"))
            us_ed.run_r_script(os.path.join("src","EIA-Form860","loadEIA860_Schedule2_Plants.R"))

        ################
        ## EIA Form 923
        ################
        if "EIA923" in products :
            print(product_dict["EIA923"])
            us_ed.run_r_script(os.path.join("src","EIA-Form923","loadGenerationAndFuel.R"))
            us_ed.run_r_script(os.path.join("src","EIA-Form923","loadGenerator.R"))
            us_ed.run_r_script(os.path.join("src","EIA-Form923","loadFuelPurchase.R"))

        ################
        ## CEMS Facilities
        ################
        if "CEMS" in products or "CEMS_Facility" in products :
            print(product_dict["CEMS_Facility"])
            us_ed.run_r_script(os.path.join("src","EPA-CEMS","loadFacilityData.R"))

        ###############
        ## Data Crosswalks
        ###############
        if "Crosswalks" in products :
            print(product_dict["Crosswalks"]) 
            us_ed.run_r_script(os.path.join("src","EPA-CEMS","CEMStoEIAMap.R"))


        #################
        ## CEMS Operations
        #################
        if "CEMS" in products or "CEMS_Operations" in products :
            print(product_dict["CEMS_Operations"]) 
            us_ed.run_r_script(os.path.join("src","EPA-CEMS","loadCEMSData.R"))
            us_ed.run_r_script(os.path.join("src","EPA-CEMS","netToGrossCalculation.R"))

        #################
        ## NRC RPSR
        #################
        if "NRC" in products :
            print(product_dict["NRC"]) 
            us_ed.run_r_script(os.path.join("src","NRC-RPSR","load-power-status-reports.R"))         

        print("Data build complete")


####################################
## processRepair
## Repair management
####################################
def processRepair(us_ed,sp_args) :
    print(
        "USElecData Repairing internal datastores"
        )

    ##Create a dictionary of source data product keys and descriptions
    datastore_dict = {
        'CEMS' : "All CEMS datastores",
        'CEMS_Operations' : 'CEMS Operations data file log'
    }

    #Respond with the list of data sources to --list
    if sp_args.list :
        print("\nDatastore List\n")
        print("Name".ljust(20),"Description")
        for k in datastore_dict :
            print(k.ljust(20),datastore_dict[k])

    else :
        print("Repairing internal datastores....")
        #Otherwise download
        if len(sp_args.datastores) == 0 or "all" in sp_args.datastores :
            datastores = datastore_dict.keys()
        else :
            datastores = []
            for s in sp_args.datastores :
                if s in datastore_dict :
                    datastores += [s]
                else :
                    print("Unknown internal datastore name",s)
                    print("Use USElecData repair --list for a list of internal datastores")
                    sys.exit(-1)
            
        ################
        ## CEMS_Operations
        ################
        if "CEMS" in datastores or "CEMS_Operations" in datastores :
            print(datastore_dict["CEMS_Operations"])
            us_ed.run_python_script(os.path.join("src","EPA-CEMS","retconSourceFileDB.py"))

        print("Repair operations complete")
            
def processPackage(us_ed,sp_args) :
    print(
        "Packaging Data",
        "",
        "WARNING: Flags are currently not supported.",
        sep="\n"
    )

    #Determine the output data format
    if sp_args.data_format == "rds" :
        print("Packing R format (.rds) files")
        export_files = "rds"
    elif sp_args.data_format == "dta" :
        print("Packing Stata format (.dta) files")
        export_files = "dta"
    elif sp_args.data_format is None or sp_args.data_format == "" :
        print("You must specify a data file format to export using the --data-format= option")
        sys.exit(-1)
    else :
        print("Unknown export file format:",sp_args.data_format)
    #Determine the file type we should export to
    filename_base, ext = os.path.splitext(sp_args.filename)

    if ext == ".zip" :
        print("Creating Zip Archive")
        archive_type = "zipfile"
    elif ext == ".gz" and os.path.splitext(filename_base) == "tar" :
        print("Creating gzipped TAR archive")
        archive_type = "gztar"
        print("This format is currently unsupported.")
        sys.exit(-1)
    else :
        print("Cannot determine the archive type from the file extension",ext)
        print("Proposed file name:", sp_args.filename)
        sys.exit(-1)

    #Check that we will be able to write to the eventual output file
    filepath, filename = os.path.split(sp_args.filename)
    if not os.path.isdir(filepath) :
        print("The ouput path",filepath,"does not exist.")
        sys.exit(-1)
        if not os.access(sp_args.filename, os.W_OK) :
            print("The output path is not writable. Check your permissions to the folder.")
            sys.exit(-1)

    #Walk the output directory
    #Returns a list of (path, folders, files) tuples 
    filelist = [f for f in os.walk(os.path.join(us_ed.outputRoot,"data","out"))]

    file_counter = 0
    with zipfile.ZipFile(sp_args.filename,mode="w") as zipout :
        #######################
        ## Add non-data files to the archive which describe the data build
        #######################
        for f in ["Python-Environment-Configuration.txt","R-Environment-Configuration.txt"] :
            zipout.write(os.path.join(us_ed.outputRoot,f),arcname=f)
        
        #Iterate the filelist
        for walker in filelist :
            #Extract the folder name
            path = walker[0]
            #Iterate the files in this folder
            for f in walker[2] :
                if "." + export_files in f :
                    fp = os.path.join(path,f)
                    zipout.write(
                        fp,
                        arcname=os.path.relpath(fp,start=os.path.join(us_ed.outputRoot,"data","out"))
                    )
                    file_counter = file_counter + 1
            
        
        



HELP_EPILOG = "For additional information see https://github.com/drofphilosophe/USElecData"

#This script is intended to be run only from the command line             
if __name__ == "__main__" :

    ##################################
    ##################################
    ## Init the argument parser
    ##################################
    ##################################        
    parser = argparse.ArgumentParser(
        description="Build and manage high frequency electricity data in the United States",
        epilog=HELP_EPILOG
        )

    parser.add_argument("--ignore-environment",action="store_true", help="Ignore the requirement to run in the USElecData Anaconda environment")
    parser.add_argument("--no-warnings",action="store_false", help="Suppress warning messages")
    
    subparsers = parser.add_subparsers(help='USElecData commands',dest='subcommand')

    #######################
    ## Initalize Configuration Files
    #######################
    sp_init = subparsers.add_parser(
        "init",
        help="Initalize configuration files",
        description="Initalize USElecData",
        epilog=HELP_EPILOG
        )
    sp_init.add_argument("--output-path","-o",type=str,help="Path to data file repository")
                         
    #######################
    ## Manage Source Data
    #######################
    sp_source = subparsers.add_parser(
        "source",
        help="Manage source data",
        description="Manage USElecData source data",
        epilog=HELP_EPILOG
        )
    sp_source.add_argument("sources",nargs="*",help="List of source data products to download")
    sp_source.add_argument("--list",action="store_true",help="Display a list of source data products")
    sp_source.add_argument("--resource",action="store_true", help="Replace all source data with fresh data")

    ######################
    ## Build Output Data
    ######################
    sp_build = subparsers.add_parser(
        "build",
        help="Build output data",
        description="Build USElecData output data",
        epilog=HELP_EPILOG
        )
    sp_build.add_argument("products",nargs="*",help="List of data products to build")
    sp_build.add_argument("--rebuild",action="store_true", help="Rebuild data from original local copies of source data")
    sp_build.add_argument("--list",action="store_true",help="Display a list of output data products")

    ######################
    ## Access repair utilities
    ######################
    sp_repair = subparsers.add_parser(
        "repair",
        help="Repair internal maintenance datastores",
        description="Repair USElecData internal datastores",
        epilog=HELP_EPILOG
        )
    sp_repair.add_argument("datastores",nargs="*",help="List of data internal datastores to repair")
    sp_repair.add_argument("--list",action="store_true",help="Display a list of output data products")
    
    
    ######################
    ## Create ouput packages
    ######################
    sp_package = subparsers.add_parser(
        "package",
        help="Create ouput data packages",
        description="Create USElecData ouput data packages",
        epilog=HELP_EPILOG
        )
    sp_package.add_argument("filename",type=str,help=".zip or .tar.gz file to export data to")
    sp_package.add_argument("--data-format",type=str,help="Data file format",choices=["rds","dta"])
    

    ######################
    ## Display the license
    ######################
    sp_license = subparsers.add_parser(
        "license",
        help="Display the software license",
        description="Display the software license for USElecData",
        epilog=HELP_EPILOG
        )
    
    ######################
    ## Cite the data/package
    ######################
    sp_citation = subparsers.add_parser(
        "citation",
        help="Display citation information",
        description="Display citation information for USElecData",
        epilog=HELP_EPILOG
        )

    sp_citation.add_argument("--bibtex",action="store_true",help="Display citation as bibtex")
        
    ######################
    ## Delete Existing Data
    ######################
    sp_delete = subparsers.add_parser(
        "delete",
        help="Remove data",
        description="Delete USElecData source or output data",
        epilog=HELP_EPILOG
        )

    #####################
    ## Parse arguments
    #####################
    args = parser.parse_args()
    
    ################
    ## Do the work here
    ## You should catch failures, terminate, and return a non-zero exit status
    ## with sys.exit()
    ################
    #Suppress warnings for certain operations
    if args.subcommand is None or args.subcommand == ["init","license","citation",""] :
        show_warnings = False
    else :
        show_warnings = args.no_warnings

    #Suppress the environment check for certain operations
    if args.subcommand is None or args.subcommand == ["citation"] :
        ignore_environment = True
    else :
        ignore_environment = args.ignore_environment

    #Pass the output root only if we're performing the init operation
    if args.subcommand == "init" :
        pass_output_path = args.output_path
    else :
        pass_output_path = None
        
    #Instantate the USElecData Class
    us_ed = USElecDataClass.USElecDataClass(
        warnings=show_warnings,
        ignore_environment=ignore_environment,
        output_path = pass_output_path)

    #If no subcommand is entered, display the usage
    if args.subcommand is None :
        us_ed.print_banner()
        parser.print_usage(file=None)
    elif args.subcommand == "license" :
        processLicense(us_ed,args)
    elif args.subcommand == "citation" :
        processCitation(us_ed,args)
    elif args.subcommand == "init" :
        processInit(us_ed,args)
    elif args.subcommand == "source" :
        processSource(us_ed,args)
    elif args.subcommand == "build" :
        processBuild(us_ed,args) 
    elif args.subcommand == "package" :
        processPackage(us_ed,args)
    elif args.subcommand == "repair" :
        processRepair(us_ed,args)
    elif args.subcommand == "delete" :
        print("Subcommand build not currently implemented")
        sys.exit(1)
    else :
        print(f"Unknown subcommand {args.subcommand}. This should never happen")
        raise Exception("Unknown subcommand")
        

    ################
    ## If you make it this far, everything executed sucessfully
    ## Return the exit status to the shell
    ################
    sys.exit(0)

##if not __main__
else :
    print("This script is designed to be run from the command line and has no functionality when run interactively.")




    
