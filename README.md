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
 
 Code in this package will download and clean uop data from many sources. After pulling the repo, you should create a local configuration file by editing `./config_local_template.yaml` to point to the local path where you will store source, intermediate, and output data. Save this file as `./config_local.yaml`. **DO NOT MAKE YOUR DATA PATH A SUBDIRECTORY OF YOUR LOCAL CODE REPOSITORY**. You should then run each of the Python/R scripts in the order described in the Markdown file in each subfolder of the `src` directory. The `src` folders should be loaded an cleaned in the following order:
 1. `tz-info`
 2. `EIA-Form860`
 3. `EIA-Form923`
 4. `EPA-CEMS`


