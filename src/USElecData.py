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
        "WARNING: Flags are currently not supported.",
        "Updating existing data....",
        sep="\n"
        )
    
    ################
    ## tz-info
    ################
    print("Time zone information")
    us_ed.run_python_script(os.path.join("src","tz-info","getTZInfo.py"))


    ################
    ## EIA Form 860
    ################
    print("EIA Form 860 - Electricity Generator Data")
    us_ed.run_python_script(os.path.join("src","EIA-Form860","getEIAForm860.py"))


    ################
    ## EIA Form 923
    ################
    print("EIA Form 923 - Fuel and Generation for Electricity Consumption")
    us_ed.run_python_script(os.path.join("src","EIA-Form923","getEIAForm923.py"))

    ################
    ## EIA Form 923
    ################
    print("EPA CEMS")
    us_ed.run_python_script(os.path.join("src","EPA-CEMS","downloadCEMSData.py"))
    us_ed.run_python_script(os.path.join("src","EPA-CEMS","downloadFacilityInfo.py"))

    print("Data download complete")
    

def processBuild(us_ed,sp_args) :
    print(
        "USElecData Building Data",
        "",
        "WARNING: Flags are currently not supported.",
        "Updating existing data....",
        sep="\n"
        )
    
    ################
    ## tz-info
    ################
    #No files
    
    ################
    ## EIA Form 860
    ################
    us_ed.run_r_script(os.path.join("src","EIA-Form860","loadEIA860_Schedule3_Generators.R"))
    us_ed.run_r_script(os.path.join("src","EIA-Form860","loadEIA860_Schedule6.R"))
    us_ed.run_r_script(os.path.join("src","EIA-Form860","loadEIA860_Schedule2_Plants.R"))

    ################
    ## EIA Form 923
    ################
    us_ed.run_r_script(os.path.join("src","EIA-Form923","loadGenerationAndFuel.R"))
    us_ed.run_r_script(os.path.join("src","EIA-Form923","loadGenerator.R"))

    ################
    ## EIA Form 923
    ################
    us_ed.run_r_script(os.path.join("src","EPA-CEMS","loadFacilityData.R"))
    us_ed.run_r_script(os.path.join("src","EPA-CEMS","loadCEMSData.R"))
    us_ed.run_r_script(os.path.join("src","EPA-CEMS","CEMStoEIAMap.R"))
    us_ed.run_r_script(os.path.join("src","EPA-CEMS","netToGrossCalculation.R"))

    print("Data build complete")



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

    with io.BytesIO() as buf :
        file_counter = 0
        with zipfile.ZipFile(buf,mode="w") as zipout :                   
            #Iterate the filelist
            for walker in filelist :
                path = walker[0]
                for f in walker[2] :
                    if "." + export_files in f :
                        fp = os.path.join(path,f)
                        zipout.write(
                            fp,
                            arcname=os.path.relpath(fp,start=os.path.join(us_ed.outputRoot,"data","out"))
                        )
                        file_counter = file_counter + 1
                        
        #Rewind the buffer
        print("Writing",file_counter,"files with a total of",buf.tell(),"bytes to\n",sp_args.filename)
        buf.seek(0)
        with open(sp_args.filename,"wb") as fout :
            fout.write(buf.read())
            
        
        



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
    sp_source.add_argument("--resource-all",action="store_true", help="Replace all source data with fresh data")
    sp_source.add_argument("--update-all","-a",action="store_true", help="Download new source data")

    ######################
    ## Build Output Data
    ######################
    sp_build = subparsers.add_parser(
        "build",
        help="Build output data",
        description="Build USElecData output data",
        epilog=HELP_EPILOG
        )
    sp_build.add_argument("--rebuild-all",action="store_true", help="Rebuild all data from local copies of source data")
    sp_build.add_argument("--update-all","-a",action="store_true", help="Update source data and build updated output files")
    
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
        
    #Instantate the USElecData Class
    us_ed = USElecDataClass.USElecDataClass(warnings=show_warnings,ignore_environment=ignore_environment)

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
        print("Subcommand build not currently implemented")
        sys.exit(1)
    elif args.subcommand == "package" :
        processPackage(us_ed,args)
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




    
