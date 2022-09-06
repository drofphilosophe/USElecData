# United States Electricity Data

This is a repository of code to download, compile and clean high-frequency electricty generation and emissions data for the United States. 

# License

<pre>
   Copyright 2022 James Archsmith

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 </pre>
 
# Using this package

Code in this package will download and clean up data from many sources. The code is designed to run in a custom Anaconda (<https://www.anaconda.com/>) environment defined by the configuration file `USElecData_conda.yaml`. To use the code in this repository you should do the following:
1. Download and install Anaconda on your system
2. Pull a copy of the repository
3. Navigate to the root directory and this repo and create an Anaconda environment using the command 

`conda env create -f USElecData_conda.yaml`

4. Activate the `USElecData` Anaconda environment using the command 

`conda activate USElecData`

5. Initialize the configuration of your environment using the command 

`USElecData init --output-path=<path_to_ouput_data>` 

This will create a local configuration file and set environment variables within your enviroment that scripts will use later. The `<path_to_output_data>` is a folder where you would like the data to be stored on your local system. Anticiapte serveral hundred gigabytes. During this process you will be prompted to enter an API key for Data.gov. This is required to access some US government data APIs. If you don't currently have an API key, they script will provide a link for you to sign up for one. 

6. Reactivate your the `USElecData` Anaconda environment using the commands 

`conda deactivate`

`conda activate USElecData`

7. Download all the original source data with the command

`USElecData source --update-all`

8. Build all output data files

`USElecData build --rebuild-all`

## Running outside Anaconda or in a different environment

It is strongly recommended you run all the code in this repository in the Anaconda environment defined by `USElecData_conda.yaml`. This provides a clean and consistent environment for all of the code, handles package management and enables several features that will hopefully streamline the build process. The code has only been tested against that environment. It will likley run outside the enviroment, but you may encounter unforseen errors. 

## Legacy Instructions

After pulling the repo, you should create a local configuration file by editing `./config_local_template.yaml` to point to the local path where you will store source, intermediate, and output data. Save this file as `./config_local.yaml`. **DO NOT MAKE YOUR DATA PATH A SUBDIRECTORY OF YOUR LOCAL CODE REPOSITORY**. You should then run each of the Python/R scripts in the order described in the Markdown file in each subfolder of the `src` directory. The `src` folders should be loaded an cleaned in the following order:
 1. `tz-info`
 2. `EIA-Form860`
 3. `EIA-Form923`
 4. `EPA-CEMS`


