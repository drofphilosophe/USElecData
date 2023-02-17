################################################
## Load boiler-stack associations from EIA Form 860 Schedule 6
################################################
library(tidyverse)
library(lubridate)
library(readxl)
library(haven)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EIA-Form860/loadEIA860_Schedule6.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

path.EIA860.source = file.path(path.project,"data","source","EIA-Form860")
path.EIA860.out = file.path(path.project,"data","out","EIA-Form860")

year.start = as.integer(project.config$sources$`EIA-Form860`$`start-year`)
if(is.null(project.config$sources$`EIA-Form860`$`end-year`)) {
  year.end <- year(today())
} else {
  year.end <- as.integer(project.config$sources$`EIA-Form860`$`end-year`)
}


##########################
## Create Output Folders
##########################
dir.create(path.EIA860.out, recursive = TRUE, showWarnings = FALSE)


#Detect the starting and ending row of data in the schedule 6 sheets
#This follows the same approach in every worksheet.
#Load all of column A. The header rows have some variant of "Utility ID"
#The table ends at the last row or in the row before we encounter 
#a value beginning with "NOTE". Return these bounds as a vector.
detect_start_end <- function(path,sheet) {
  #Determine the starting and ending row of data
  #By reading the first column as text
  read_excel(
    path,
    sheet=sheet,
    range=cell_limits(c(1, 1), c(NA, 1)),
    col_types=c("text"),
    col_names="A"
  ) %>%
    mutate(n=row_number()) -> one.col
  
  ##The first row of data is the column names
  ##It should be some variant of "Utility ID"
  one.col %>%
    filter(str_detect(str_to_upper(A),"UTILITY")) %>%
    summarize(n=min(n)) %>%
    pull(n) -> first.row
  
  #Abort if we can't detect row headers
  if(first.row == Inf) 
      stop(
        str_c(
          "Unable to detect row headers in ",
          "\nfile: ", path,
          "\nsheet: ", sheet
          )
        )
  
  #There is frequently a "note" column at the end.
  #Read column 1 only and look for the note column 
  #This generates a warning if there is no note. 
  #We'll suppress that
  suppressWarnings(
    one.col %>%
      filter(str_detect(A,"NOTE")) %>%
      summarize(n=min(n)) %>%
      pull(n) -> last.row
  )
  
  
  #print(last.row)
  #the max() function above will return Inf, but we 
  if(last.row==Inf) last.row = as.integer(NA)
  
  return(c(first.row,last.row))
}









###########################
###########################
## Define functions for identifying 
## and parsing the various parts of Form 6
## Feed each a path to the file and a year
## It will return a tibble of data or an empty tibble
###########################
###########################

##############################
#Boiler to generator mappings
##############################
load_boiler_generator <- function(path,yr) {
  sheetlist <- excel_sheets(path)
  
  #Find a sheet with "Boiler" and "Gen(erator)" in the name
  sheetlist <- sheetlist[ str_detect(sheetlist,"[Bb]oiler") & str_detect(sheetlist,"[Gg]en") ]
  
  if(length(sheetlist) == 0) {
    warning(str_c("Could not find a Boiler-Generator sheet in ",yr,". Skipping."))
    return(tibble())
  }
  
  if(length(filelist) > 1) {
    warning(str_c("Multiple sheets in ",yr," match a Boiler-Generator sheet name. Skipping."))
    return(tibble())
  }
  
  #Determine the starting and ending row of data
  row.bounds = detect_start_end(path,sheetlist)
  
  #Read the sheet and clean up the data
  read_excel(
    file.path(path),
    sheet=sheetlist, 
    range= cell_rows(c(row.bounds[1], row.bounds[2]-1))
  ) %>%
    #Conform column names
    rename(
      orispl.code = matches("PLANT.?CODE",ignore.case = TRUE),
      eia.unit.id = contains("BOILER",ignore.case=TRUE),
      eia.generator.id = matches("GENERATOR.ID",ignore.case=TRUE)
    ) %>%
    select(orispl.code,eia.unit.id,eia.generator.id) %>%
    mutate(
      year = yr
    ) %>%
    return() 
}








##################################
## Boiler-Stack/Flue mapping
##################################
load_boiler_stack <- function(path,yr) {
  
  sheetlist <- excel_sheets(path)
  
  #Find a sheet with "Boiler" and "Stack" in the name
  sheetlist <- sheetlist[ str_detect(sheetlist,"[Bb]oiler") & str_detect(sheetlist,"[Ss]tack") ]
  
  if(length(sheetlist) == 0) {
    warning(str_c("Could not find a Boiler-Stack-Flue sheet in ",yr,". Skipping."))
    return(tibble())
  }
  
  if(length(filelist) > 1) {
    warning(str_c("Multiple sheets in ",yr," match a Boiler-Stack-Flue sheet name. Skipping."))
    return(tibble())
  }
  
  #Determine the starting and ending row of data
  row.bounds = detect_start_end(path,sheetlist)
  
  #Read the sheet and clean up the data
  read_excel(
    file.path(path),
    sheet=sheetlist, 
    range= cell_rows(c(row.bounds[1], row.bounds[2]-1))
  ) %>%
    #Conform column names
    rename(
      orispl.code = matches("PLANT.?CODE",ignore.case = TRUE),
      eia.unit.id = contains("BOILER",ignore.case=TRUE),
      eia.stack.id = contains("STACK",ignore.case=TRUE)
    ) %>%
    select(orispl.code,eia.unit.id,eia.stack.id) %>%
    mutate(
      year = yr
    ) %>%
    return()
}




##Init accumulators for each of the parts of Schedule 6
all.boiler.stack <- tibble()
all.boiler.generator <- tibble()


for(yr in year.start:year.end) {
  #Does this folder exist?
  if(!dir.exists(file.path(path.EIA860.source,yr))) {
    warning(str_c("Source data folder for ",yr," does not exist. Skipping"))
    next
  }
  
  #List files in this folder
  filelist <- list.files(file.path(path.EIA860.source,yr))
  
  if(length(filelist) == 0) {
    warning(str_c("No files found in source data folder for ",yr,". Skipping."))
    next
  }
  
  
  ##########################
  ##########################
  ## Identify the Schedule 6 file
  ##########################
  ##########################
  
  ######################
  ## This block implements logic to identify the Form 6 file
  ## The file names vary by year, so there will be code blocks
  ## for sets of years where the filenames follow similar patterns
  ######################
  if(yr >= 2009) {
    ## 2009 and later - look for files with "Enviro" and "Assoc" in their name
    filelist <- filelist[str_detect(filelist,"Enviro") & str_detect(filelist,"Assoc")]
    ##Remove any files with tildes in their names. These are temporary excel files
    filelist <- filelist[!str_detect(filelist,"\\~")]
  } else {
    #Catch all years not handled by this block
    warning(str_c("We're not currently equipped to handle files from ", yr,". Skipping"))
    next
  }
  
  ##Check that we have only exactly one file in the filelist
  ##We should always be able to identify exactly one file.
  ##Otherwise, throw an error
  if(length(filelist) == 0) {
    stop(str_c("Files exist in source data folder for ",yr," but none match a Form 6 data file name."))
    next
  }
  
  if(length(filelist) > 1) {
    stop(str_c("Multiple files in source data folder for ",yr," match a Form 6 data file name."))
    next
  }
  
  
  ##########################
  ## Process each part of schedule 6
  ##########################
  load_boiler_generator(file.path(path.EIA860.source,yr,filelist),yr) %>%
    bind_rows(all.boiler.generator) -> all.boiler.generator

    load_boiler_stack(file.path(path.EIA860.source,yr,filelist),yr) %>%
    bind_rows(all.boiler.stack) -> all.boiler.stack
}









####################################################
####################################################
####################################################
## Write Output Files
####################################################
####################################################
####################################################



###############

###############
if("rds" %in% project.local.config$output$formats) {
  dir.create(file.path(path.EIA860.out,"rds"),showWarnings = FALSE)
  
  ## Boiler-Stack
  all.boiler.stack %>%
    write_rds(
      file.path(path.EIA860.out,"rds","Form860_Schedule6_BoilerStack.rds.gz"), 
      compress="gz"
    )
  
  ## Boiler-Generator
  all.boiler.generator %>%
    write_rds(
      file.path(path.EIA860.out,"rds","Form860_Schedule6_BoilerGenerator.rds.gz"), 
      compress="gz"
    )
}

if("dta" %in% project.local.config$output$formats) {
  dir.create(file.path(path.EIA860.out,"stata"),showWarnings = FALSE)
  
  ## Boiler-Stack
  all.boiler.stack %>%
    rename_all(.funs=list( ~ str_replace_all(.,"[\\.\\s]", "_"))) %>%
    write_dta(file.path(path.EIA860.out,"stata","Form860_Schedule6_BoilerStack.dta"))
  
  ## Boiler-Generator
  all.boiler.generator %>%
    rename_all(.funs=list( ~ str_replace_all(.,"[\\.\\s]", "_"))) %>%
    write_dta(file.path(path.EIA860.out,"stata","Form860_Schedule6_BoilerGenerator.dta"))
}



