################################################
## Load boiler-stack associations from EIA Form 860 Schedule 6
################################################
library(tidyverse)
library(lubridate)
library(readxl)
library(haven)
library(here)
library(yaml)
library(jsonlite)
library(glue)
library(usmap)
library(fuzzyjoin)
library(sf)

#Identifies the project root path using the
#relative location of this script
i_am("src/EIA-Form860/loadEIA860_Schedule2_Plants.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

path.EIA860.source = file.path(path.project,"data","source","EIA-Form860")
path.EIA860.out = file.path(path.project,"data","out","EIA-Form860")
path.tzinfo = file.path(path.project,"data","source","tz-info")

year.start = as.integer(project.config$sources$`EIA-Form860`$`start-year`)
if(is.null(project.config$sources$`EIA-Form860`$`end-year`)) {
  year.end <- year(today())
} else {
  year.end <- as.integer(project.config$sources$`EIA-Form860`$`end-year`)
}


############################
## Load a JSON file with column name remapping information
## Each top-level entry in this list is a conformed column name
## associated with a list with many candidate original names
## We'll use this information to conform the myriad column names
## that show up in the original EIA data
###########################
read_json(
  here(
    file.path("src","EIA-Form860","EIA860_Schedule2_Plants_Colspec.json")
  )
) -> colspec

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





all.data <- tibble()
for(yr in year.start:year.end) {
  #Does this folder exist?
  if(!dir.exists(file.path(path.EIA860.source,yr))) {
    warning(glue("Source data folder for {yr} does not exist. Skipping"))
    next
  }
  
  #List files in this folder
  filelist <- list.files(file.path(path.EIA860.source,yr))
  
  if(length(filelist) == 0) {
    warning(glue("No files found in source data folder for {yr}. Skipping."))
    next
  }
  
  
  ##########################
  ##########################
  ## Identify the Schedule 2 file
  ##########################
  ##########################
  
  ######################
  ## This block implements logic to identify the Form 6 file
  ## The file names vary by year, so there will be code blocks
  ## for sets of years where the filenames follow similar patterns
  ######################
  if(yr >= 2009) {
    ## 2009 and later - look for files with "Enviro" and "Assoc" in their name
    filelist <- filelist[str_detect(filelist,"Plant")]
    ##Remove any files with tildes in their names. These are temporary excel files
    filelist <- filelist[!str_detect(filelist,"\\~")]
  } else {
    #Catch all years not handled by this block
    warning(glue("We're not currently equipped to handle files from {yr}. Skipping"))
    next
  }
  
  ##Check that we have only exactly one file in the filelist
  ##We should always be able to identify exactly one file.
  ##Otherwise, throw an error
  if(length(filelist) == 0) {
    stop(str_c("Files exist in source data folder for {yr} but none match a Form 2 data file name."))
    next
  }
  
  if(length(filelist) > 1) {
    stop(str_c("Multiple files in source data folder for {yr} match a Form 2 data file name."))
    next
  }
  
  
  ##########################
  ## Process Plant Data in schedule 2 - There should be only one sheet
  ##########################
  xlfile = file.path(path.EIA860.source,yr,filelist)
  sheet = excel_sheets(xlfile)
  if(length(sheet) > 1) {
    writeLines(glue("Detected more than one sheet in the Plant file in year {yr}."))
    writeLines("Here is a list of the sheet names:")
    writeLines(str_c(sheet,sep=" -- "))
    stop("Aborting")
  }
  

  row.bounds = detect_start_end(xlfile,sheet)
  read_excel(
    xlfile,
    sheet=sheet,
    range=cell_rows(c(row.bounds[1], row.bounds[2]-1)),
    col_types = "text"
  ) -> sheet.data
  
  orig.names <- names(sheet.data)
  new.names <- names(colspec)
  for(o in orig.names) {
    for(n in new.names) {
      n.list = unlist(colspec[[n]])
      if(o %in% n.list) {
        #Rename the old name to the new name
        sheet.data %>% rename(!!n := rlang::sym(o)) -> sheet.data
        #Pop the new name from new.names
        new.names = new.names[new.names != n]
        #Pop the old name from orig.names
        orig.names = orig.names[orig.names != o]
      }
    }
  }
  
  writeLines("\n\n")
  writeLines("------------------------------------------------------------")
  writeLines(crayon::bold(str_c("Year: ", yr, " Sheet: ", sheet)))
  writeLines("------------------------------------------------------------")
  writeLines("The following conformed column names were not found in this file.")
  writeLines("This is typical as the data content changes over time.")
  for(n in new.names) {
    writeLines(n)
  }
  
  writeLines("")
  writeLines("The following columns in the data file did not have a conformed name")
  writeLines("They will be dropped.")
  for(n in orig.names) {
    writeLines(n)
  }
  
  sheet.data %>%
    select(-any_of(orig.names)) %>%
    mutate(
      year = yr,
    ) %>%
    bind_rows(all.data) -> all.data
  
}

##########################
## Here you will clean up the data types
## Everything has been read in as text to avoid
## inconsistent guessing of data types
#########################
numeric.cols = c(
  "utility.id",
  "orispl.code",
  "plant.latitude",
  "plant.longitude"
)
for(c in numeric.cols) {
  #Display values that won't recast to a numeric
  all.data %>%
    mutate(
      temp.xxx = !!rlang::sym(c)
    ) %>%
    drop_na(temp.xxx) %>%
    filter(is.na(as.numeric(temp.xxx))) %>%
    filter( !( str_trim(temp.xxx) %in% c("","."))) %>%
    distinct(temp.xxx) -> bad.vals
  
  if(nrow(bad.vals) > 0) {
    writeLines(str_c("Column ",crayon::bold(c)," should be numeric but contains the following non-numeric values:"))
    print(bad.vals,n=Inf)
  }
  
  all.data %>%
    mutate(!!c := as.numeric(!!rlang::sym(c))) -> all.data
}


##Clean up county names
all.data %>% 
  distinct(plant.state,plant.county) %>%
  mutate(
    fips = as.character(NA),
    plant.county.clean = as.character(NA)
    ) -> state.county

state.county %>% distinct(plant.state) %>% drop_na() %>% pull(plant.state) -> state.list

countypop %>% 
  select(abbr,county,fips) %>%
  mutate(
    county = str_replace(county," County$",""),
    county = str_replace(county, " Parish$","")
  ) %>%
  rename(plant.county = county) -> counties

for(st in state.list) {
  writeLines(glue("Cleaning county names in state {st}"))
  state.county %>%
    select(-plant.county.clean,-fips) %>%
    filter(plant.state == st) %>%
    stringdist_join(
      filter(counties,abbr==st),
      by = "plant.county",
      mode = "left",
      ignore_case = TRUE,
      method = "jw",
      max_dist = 99,
      distance_col = "dist"
    ) %>%
    rename(
      plant.county = plant.county.x,
      plant.county.clean = plant.county.y
    ) %>%
    group_by(plant.county) %>%
    arrange(plant.county,dist,fips) %>%
    slice(1) %>%
    ungroup() %>%
    select(plant.state,plant.county,fips,plant.county.clean) %>%
    bind_rows(
      filter(state.county,plant.state != st)
    ) -> state.county
}

all.data %>%
  left_join(
    state.county,
    by=c("plant.state","plant.county")
    ) %>%
  select(-plant.county) %>%
  rename(
    county.fips = fips,
    plant.county = plant.county.clean
  ) -> all.data



##FIPS codes
all.data %>%
  mutate(
    plant.state.fips = fips(plant.state),
  ) -> all.data


#Determine the time zone of each plant using tz-info
st_read(file.path(path.tzinfo,"tz-geodata-combined-with-oceans.json")) -> tzinfo.geo

sf_use_s2(FALSE)
all.data %>%
  group_by(orispl.code) %>%
  summarize(
    latitude = median(plant.latitude, na.rm=TRUE),
    longitude = median(plant.longitude,na.rm=TRUE)
  ) %>%
  drop_na() %>%
  st_as_sf(coords=c("longitude","latitude"),crs=st_crs(4326)) %>%
  st_join(tzinfo.geo) %>%
  rename(time.zone = tzid) %>%
  st_drop_geometry() %>%
  right_join(all.data,by="orispl.code") -> all.data


####################################################
####################################################
####################################################
## Write Output Files
####################################################
####################################################
####################################################
all.data %>%
  arrange(year,orispl.code) -> all.data


###############

###############
if("rds" %in% project.local.config$output$formats) {
  dir.create(file.path(path.EIA860.out,"rds"),showWarnings = FALSE)
  
  ## Boiler-Stack
  all.data %>%
    write_rds(
      file.path(path.EIA860.out,"rds","Form860_Schedule2_Plant.rds.gz"),
      compress="gz"
    )
  
}

if("dta" %in% project.local.config$output$formats) {
  dir.create(file.path(path.EIA860.out,"stata"),showWarnings = FALSE)
  
  ## Boiler-Stack
  all.data %>%
    rename_all(.funs=list( ~ str_replace_all(.,"[\\.\\s]", "_"))) %>%
    rename_all(.funs=list(~str_sub(.,1,31))) %>%
    write_dta(file.path(path.EIA860.out,"stata","Form860_Schedule2_Plant.dta"))
  
}