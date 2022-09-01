#############################
## This script will install all the required R libraries
## for the USElecData repository. It is intended to be 
## run inside a conda enviroment with R installed
#############################
library(here)
library(yaml)
library(readr)
library(glue)
library(lubridate)

#Identifies the project root path using the
#relative location of this script
i_am("src/init/initialize_R_environment.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`


#These are the libraries required to 
#Run all the scripts in this repo
required.libraries = c(
  "tidyverse", #r-tidyverse
  "lubridate", #r-lubridate
  "readxl", #r-readxl
  "haven", #r-haven
  "here", #r-here
  "yaml", #r-yaml
  "jsonlite", #r-jsonlite
  "glue", #r-glue
  "usmap", #r-usmap
  "fuzzyjoin", #NOT IN CONDA
  "assertr", #NOT IN CONDA
  "magrittr", #r-magrittr
  "units", #r-units
  "matrixStats", #r-matrixstats
  "tcltk", #r-tcltk2
  "sf", #r-sf
  "curl" #r-curl
)

#The following scripts cannot be installed 
#by Anaconda when it builds an R environment
to.install.libraries = c(
  "fuzzyjoin", 
  "assertr"
)

#Install needed libraries
for(lib in to.install.libraries) {
  install.packages(lib)
}

#Write a file detailing the R configuration
file.out = file.path(path.project,"R-Environment-Configuration.txt")
write_lines(
  c(
    "USElecData",
    glue("Code Version: {project.config$`version-info`$`version-number`}"),
    glue("Code Version Date: {project.config$`version-info`$`version-date`}"),
    glue("Generated {today()}"),
    "",
    "R Environment Configuration"
  ),
  file.out,
  append=FALSE
)

ver = R.Version()
for(i in names(ver)) {
  write_lines(glue("{i}\t\t{ver[i]}"),file.out,append=TRUE)
}

write_lines(
  c(
    "",
    "Package Versions:"
  ),
  file.out,
  append=TRUE
)

for(lib in required.libraries) {
  write_lines(glue("{lib}\t\t{packageVersion(lib)}"),file.out,append=TRUE)
}
