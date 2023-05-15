rm(list=ls())
library(tidyverse)
library(lubridate)
library(sf)
library(haven)
library(magrittr)
library(glue)
library(here)
library(yaml)
library(arrow)

#Identifies the project root path using the
#relative location of this script
i_am("src/EPA-CEMS/loadFacilityData.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

path.hourly.source = file.path(path.project, "data","source", "EPA-CEMS", "hourly")
path.facility.out = file.path(path.project, "data","out", "EPA-CEMS","facility_data")
path.cems.intermediate = file.path(path.project,"data","intermediate","EPA-CEMS")
path.facility.intermediate = file.path(path.project,"data","intermediate","EPA-CEMS","facility_data")

date.start <- ymd(str_c(project.config$sources$`EPA-CEMS`$`start-year`,"-1-1"))
if(is.null(project.config$sources$`EPA-CEMS`$`end-year`)) {
  date.end <- today()
} else {
  date.end <- ymd(str_c(project.config$sources$`EPA-CEMS`$`end-year`,"-12-31"))
}

############################
## Process arguments
############################
cmdline_args = commandArgs(trailingOnly=TRUE)

#Set this flag to TRUE to rebuild all files
if("--rebuild" %in% cmdline_args) {
  REBUILD_FLAG = TRUE
} else {
  REBUILD_FLAG = FALSE
}



#########################
## Create output data folders
#########################
path.hourly.out <- file.path(path.project,"data","out","EPA-CEMS","hourly")
path.monthly.out <- file.path(path.project,"data","out","EPA-CEMS","monthly")
path.daily.out <- file.path(path.project,"data","out","EPA-CEMS","daily")

for(f in c(path.hourly.out,path.monthly.out,path.daily.out)) {
  dir.create(f,recursive=TRUE,showWarnings = FALSE)
  
  if("rds" %in% project.local.config$output$formats) 
      dir.create(file.path(f,"rds"),showWarnings = FALSE)
  
  if("dta" %in% project.local.config$output$formats)
      dir.create(file.path(f,"stata"),showWarnings = FALSE)
}


#################################
## Load the file processing logs
#################################
#Load the downloaded file log and compile a list of source files
#And their last downloaded times
read_csv(
  file.path(path.hourly.source,"download-log.csv"),
  col_types=cols(
    filename = col_character(),
    year = col_integer(),
    quarter = col_integer(),
    csv_size = col_double(),
    sourceDatetime = col_datetime(),
    downloadDatetime = col_datetime(),
    csv_md5hash = col_character()
  )
) -> input.file.log

##Load the processed file log (if it exists)
path.processed.file.log = file.path(path.hourly.out,"processed-files.csv")
if(file.exists(path.processed.file.log)) {
  read_csv(
    path.processed.file.log,
    col_types=cols(
      filename = col_character(),
      year = col_integer(),
      quarter = col_integer(),
      sourceDatetime = col_datetime(),
      processedDatetime = col_datetime()
    )
  ) -> processed.file.log 
} else {
  processed.file.log <- tibble()
}

#Compile a list of files we need to process
if(nrow(processed.file.log)==0 | REBUILD_FLAG) {
  input.file.log %>% 
    mutate(
      processedDatetime = as_datetime(NA),
      sourceDatetime.processed = as_datetime(NA),
      to_process = TRUE
    ) -> files.to.process
} else {
  input.file.log %>%
    full_join(
      processed.file.log,
      by="filename",
      suffix=c("",".processed")
      ) %>%
    mutate(
      #Define the files we need to process
      to_process = case_when(
        #Source file not processed
        is.na(processedDatetime) ~ TRUE,
        #Source file downloaded more recently than last process time
        processedDatetime < downloadDatetime ~ TRUE,
        #Source file EIA timestamp before processed file EIA timestamp
        #Not sure why this would ever come up
        sourceDatetime.processed < sourceDatetime ~ TRUE,
        #All other files are up-to-date
        TRUE ~ FALSE
      )
    ) %>%
    arrange(year,quarter) -> files.to.process
}


writeLines("Data to be Updated")
files.to.process %>%
  filter(to_process) %>%
  select(filename,sourceDatetime,processedDatetime) %>%
  print(n=Inf)




#bad.orispl.list <- c(60345L,60926L,60927L)
bad.orispl.list <- c()

#Define a list of possible levels for the *_MEASURE_FLG variables
#So we can demote them to factors
MEASURE_FLG.levels = c(
  "LME", 
  "Measured", 
  "Measured and Substitute", 
  "Other", 
  "Substitute", 
  "Calculated",
  "Unknown Code",
  "Undetermined",
  "Not Applicable")

#Define a list of column names.
column.names = c(
  "state",
  "plant.name.cems",
  "orispl.code",
  "cems.unit.id",
  "associated.stacks",
  "OP_DATE",
  "OP_HOUR",
  "operational.time",
  "gross.load",
  "steam.load",
  "SO2.mass.lbs",
  "SO2.mass.measure.flag",
  "SO2.rate.lbspermmbtu",
  "SO2.rate.measure.flag",
  "CO2.mass.tons",
  "CO2.mass.measure.flag",
  "CO2.rate.tonspermmbtu",
  "CO2.rate.measure.flag",
  "NOx.rate.lbspermmbtu",
  "NOx.rate.measure.flag",
  "NOx.mass.lbs",
  "NOx.mass.measure.flag",
  "heat.input.mmbtu",
  "heat.input.measure.flag",
  "fuel.type.primary",
  "fuel.type.secondary",
  "unit.type",
  "SO2.controls",
  "NOx.controls",
  "PM.controls",
  "Hg.controls",
  "program.code"
)

#Define data types for each column. Being explicit speeds loading
#reduces import errors, and actually makes the following code cleaner.
column.spec = cols(
  state = col_character(),
  plant.name.cems = col_character(),
  orispl.code = col_integer(),
  cems.unit.id = col_character(),
  associated.stacks = col_character(),
  OP_DATE = col_character(),
  OP_HOUR = col_integer(),
  operational.time = col_double(),
  gross.load = col_double(),
  steam.load = col_double(),
  SO2.mass.lbs = col_double(),
  SO2.mass.measure.flag = col_factor(MEASURE_FLG.levels),
  SO2.rate.lbspermmbtu = col_double(),
  SO2.rate.measure.flag = col_factor(MEASURE_FLG.levels),
  NOx.rate.lbspermmbtu = col_double(),
  NOx.rate.measure.flag = col_factor(MEASURE_FLG.levels),
  NOx.mass.lbs = col_double(),
  NOx.mass.measure.flag = col_factor(MEASURE_FLG.levels),
  CO2.mass.tons = col_double(),
  CO2.mass.measure.flag = col_factor(MEASURE_FLG.levels),
  CO2.rate.tonspermmbtu = col_double(),
  CO2.rate.measure.flag = col_factor(MEASURE_FLG.levels),
  heat.input.mmbtu = col_double(),
  heat.input.measure.flag = col_character(),
  fuel.type.primary = col_character(),
  fuel.type.secondary = col_character(),
  unit.type = col_character(),
  SO2.controls = col_character(),
  NOx.controls = col_character(),
  PM.controls = col_character(),
  Hg.controls = col_character(),
  program.code = col_character()
)

#Load orispl.code to time zone mappings
read_parquet(file.path(path.facility.intermediate,"CEMS_Facility_time_zones.parquet")) %>%
  mutate(tz.name = as.character(tz.name)) %>%
  select(orispl.code,tz.name) %>%
  arrange(orispl.code) -> tz.map

#Make a list of facilities that have time zone information
tz.map %>%
  select(orispl.code) %>%
  mutate(has.tz = TRUE) -> orispl.with.tz

read_parquet(
  file.path(path.facility.out,"CEMS_Facility_Attributes.parquet")
  ) %>% 
  distinct(orispl.code, plant.name.cems,year) -> facility.names



#Storage for all monthly operations
all.monthly.data <- tibble()

#Loop through each quarterly file
files.to.process %>% 
  arrange(year,quarter) %>% 
  rowwise() -> files.to.process

for(fileinfo in group_split(files.to.process)) {
  #Extrat the current row of the tibble to a list
  fi <- as.list(fileinfo)
  
  #Construct the input and output filenames
  infile_name = glue("{fi$filename}.gz")
  outfile_name = glue("CEMS-{fi$year}Q{fi$quarter}")
  #If we need to process a quarterly file, do it here
  if(fi$to_process) {
    writeLines(glue("Processing source data file {fi$filename}"))
    
    #Make a list of facility names for this year
    facility.names %>%
      filter(year == fi$year) %>%
      group_by(plant.name.cems) %>%
      summarize(
        orispl.code = max(orispl.code),
        n = n(),
        .groups="drop"
      ) %>%
      filter(n == 1) %>%
      ungroup() %>%
      select(-n) -> facility.names.current.year
    
    #Read the compressed CSV. 
    #Skip the first row since I provided column names
    read_csv(
      file.path(path.hourly.source,infile_name), 
      col_names=column.names, 
      col_types=column.spec, 
      skip=1
    ) %>%
      #Sort order will be important later when we fix
      #DST errors. Create a record of the original order
      #in the data.
      #Convert NA values for steam.load to zeros
      mutate(
        row.id = row_number(),
        steam.load = ifelse(is.na(steam.load),0,steam.load)
      ) -> data.raw
    
    
    ##########################################
    ##########################################
    ### BAD ORISPL PROCESSING
    ##########################################
    ##########################################
    ## There are cases where the wrong ORISPL code is reported for a 
    ## plant. We're going to match the ORISPL to our cannonical list
    ## of ORISPL codes then deal with plants that don't match
    ##########################################
    writeLines("ORISPL Processing and Repair")
    ##Join cannonical ORISPL list
    data.raw %>%
      left_join(orispl.with.tz, by="orispl.code") -> data.raw
    
    #Look for ORISPL codes that didn't match
    data.raw %>%
      filter(is.na(has.tz)) %>% 
      group_by(orispl.code) %>% 
      summarize(n=n(),.groups="drop") -> bad.orispl
    
    #Only do the following if we need to fix ORISPL codes
    if( nrow(bad.orispl) > 0 ) {
      ###########################################
      ## Phase 1 - Try to match the plants by name
      ###########################################
      
      #Take the list of bad ORISPLs and try to match them to the facility
      #data based on year and name
      bad.orispl %>%
        inner_join(data.raw,by="orispl.code") %>%
        select(orispl.code,plant.name.cems) %>%
        inner_join(facility.names.current.year, by="plant.name.cems", suffix=c("",".match")) %>%
        distinct(orispl.code, orispl.code.match) -> orispl.update 
      
      #Check to see if we can update any ORISPL codes by name
      if( nrow(orispl.update) > 0) {
        print("Updating the following ORISPL codes using facility name:")
        print(orispl.update)
        
        #Merge in replacement ORISPL codes
        data.raw %>%
          left_join(orispl.update, by="orispl.code") %>%
          mutate(
            orispl.code = if_else(
              is.na(has.tz) & !is.na(orispl.code.match), 
              as.integer(orispl.code.match),
              orispl.code
            ),
            has.tz = if_else(!is.na(has.tz) | !is.na(orispl.code.match), TRUE, as.logical(NA) )
          ) %>%
          select(-orispl.code.match) -> data.raw
      } else {
        print("Unable to automatically update any ORISPL codes using the facility name")
      }
      
      
      ###################################
      ## Phase 2 - Manual update
      ##
      ## If automated matching doesn't work, do manual matching
      ##################################
      #Look for plants that still don't match
      data.raw %>%
        filter(is.na(has.tz)) %>% 
        group_by(orispl.code) %>% 
        summarize(n=n()) -> bad.orispl
      
      if(nrow(bad.orispl) > 0) {
        data.raw %>%
          mutate(
            orispl.code.new = case_when( 
              #Colorado, Brush 2/4
              fi$year >= 2005 & orispl.code == 55209 ~ 10682L, 
              #Default to the original ORISPL code
              TRUE ~ as.integer(NA)
            )
          ) -> data.raw        
        
        
        if(fi$year==2005) {
          data.raw %>%
            mutate(
              orispl.code.new = case_when(
                #Donald Von Raesfeld Power Plant
                orispl.code == 8058 ~ 56026L,
                #Norwich, CT Combustion Turbine
                orispl.code == 880022 ~ 581L,
                #DTE/General motors Pontiac
                orispl.code == 880081 ~ 10111L,
                #Edgecombe Genco. Found using UnitID and this is the only orispl not in the data
                orispl.code == 50468 ~ 10384L,
                #Plant at An Ohio State University
                orispl.code == 14013 ~ 50044L,
                #Jasper COunty Generating Facility
                orispl.code == 7996 ~ 55927L,
                #Sheboygan Falls Energy Facility
                orispl.code == 56186 ~ 56166L,
                #UUC South Charlston, WV
                orispl.code == 880026 ~ 50151L,
                #Default to the original ORISPL code
                TRUE ~ as.integer(NA)
              )
            ) -> data.raw
        } else if(fi$year==2006) {
          # No exceptions       
        } else if(fi$year==2007) {
          # No exceptions        
        } else  {
          # No exceptions         
        }
        
        #Show a list of codes we updated and then perform the update
        print("ORISPL codes updated manually:")
        data.raw %>%
          filter(is.na(has.tz)) %>%
          distinct(orispl.code, orispl.code.new) %>%
          print(n=Inf)
        
        #Perform the update
        data.raw %>%
          mutate(
            orispl.code = if_else(is.na(has.tz) & !is.na(orispl.code.new), orispl.code.new, orispl.code),
            has.tz = if_else(!is.na(has.tz) | !is.na(orispl.code.new), TRUE, NA)
          ) %>%
          select(-orispl.code.new) -> data.raw
        
        
        
        ############################################
        ## Phase 3 - break
        ## If automated and manual matching don't work, 
        ## we need to break so the manual matching list can be updated
        ############################################
        
        ##Look for ORISPL codes that don't match the facility data file
        data.raw %>%
          filter(is.na(has.tz)) %>% 
          group_by(orispl.code, plant.name.cems) %>% 
          summarize(n=n(),.groups="drop") -> bad.orispl
        
        
        if(nrow(bad.orispl) > 0) {
          print(sprintf("Bad plant codes in %s",f))
          print(bad.orispl,n=Inf)
          stop("Terminating")
        }
      }
    }
    
    
    ##########################################
    ##########################################
    ### TIME ZONE PROCESSING
    ##########################################
    ##########################################
    ## All times in CEMS are reported in local standard time
    ## Since some places change time zones over time, I've mapped
    ## each plant to an Olson name and I use these to compute the 
    ## local time offset. It's a little tricky because lubridate
    ## will want to account for DST when using local times. I get 
    ## around this by computing the local STANDARD time offset
    ## on each day at 12 AM, converting OP_HOUR and OP_DATE to 
    ## a UTC time then subtracting the offset.
    ###########################################
    writeLines("Processing time zone data")
    data.raw %>%
      #join time zone names by plant. The Olson names are time-invariant
      left_join(tz.map, by="orispl.code") -> data.raw
    
    data.raw %>%
      distinct(tz.name) %>%
      pull(tz.name) -> tz.name.list
    
    #Loop over each Olsen name and perform some calculations
    this.file <- tibble()
    for(t in tz.name.list) {
      data.raw %>%
        filter(tz.name == t) %>%
        #Times are recorded in local standard time
        mutate(
          datetime.lst = ymd_h(str_c(OP_DATE,OP_HOUR,sep=" "), tz=t),
          #Since the time is recorded in LST, when in DST, we need to add an hour to the clock
          #That will give us local clock time
          datetime.lt = if_else(dst(datetime.lst),datetime.lst + hours(1),datetime.lst),
          #Compute the corrisponding time in UTC (coverting local clock time to UTC)
          datetime.utc = with_tz(datetime.lt,tz="UTC"),
          #Remove time zone information from datetime.lst and datetime.lt so we can compute offsets
          datetime.lst = force_tz(datetime.lst,tz="UTC"),
          datetime.lt = force_tz(datetime.lt,tz="UTC"),
          #Compute offsets
          utc.offset.std.time = as.integer(difftime(datetime.lst,datetime.utc,units="hours")),
          utc.offset.local.time = as.integer(difftime(datetime.lt,datetime.utc,units="hours"))
        ) %>%
        #Remove extra columns
        select(-OP_DATE,-OP_HOUR,-tz.name,-datetime.lst,-datetime.lt,-plant.name.cems) %>%
        #Append to this.file
        rbind(this.file) -> this.file
    }
    
    
    ##########################################
    ##########################################
    ### EMISSIONS DATA REPAIRS
    ##########################################
    ##########################################
    writeLines("Repairing missing or invalid emissions data")
    this.file %>%
      #Compute rates using emissions if the rate is missing or zero. 
      #Don't do this if heat input is zero
      mutate(
        SO2.rate.lbspermmbtu = if_else(
          heat.input.mmbtu > 0 & (is.na(SO2.rate.lbspermmbtu) | SO2.rate.lbspermmbtu < 0), 
          SO2.mass.lbs / heat.input.mmbtu, 
          SO2.rate.lbspermmbtu
        ),
        NOx.rate.lbspermmbtu = if_else(
          heat.input.mmbtu > 0 & ( is.na(NOx.rate.lbspermmbtu) | NOx.rate.lbspermmbtu < 0), 
          NOx.mass.lbs / heat.input.mmbtu, 
          NOx.rate.lbspermmbtu
        ),
        CO2.rate.tonspermmbtu = if_else(
          heat.input.mmbtu > 0 & (is.na(CO2.rate.tonspermmbtu) | CO2.rate.tonspermmbtu <= 0), 
          CO2.mass.tons / heat.input.mmbtu, 
          CO2.rate.tonspermmbtu
        ),
        #CO2 rates should never be zero. Could be true with the other rates
        CO2.rate.tonspermmbtu = if_else(CO2.rate.tonspermmbtu <= 0, as.double(NA),CO2.rate.tonspermmbtu)
      ) %>%
      #Fill emissions rates down for each unit when there are NAs
      group_by(orispl.code,cems.unit.id) %>% 
      arrange(orispl.code,cems.unit.id,datetime.utc) %>%
      fill(SO2.rate.lbspermmbtu, NOx.rate.lbspermmbtu, CO2.rate.tonspermmbtu, .direction="downup") %>%
      #Impute emissions using rates and heat input
      mutate(
        SO2.mass.lbs = if_else(SO2.mass.lbs <= 0 | is.na(SO2.mass.lbs), SO2.rate.lbspermmbtu * heat.input.mmbtu, SO2.mass.lbs),
        NOx.mass.lbs = if_else(NOx.mass.lbs <= 0 | is.na(NOx.mass.lbs), NOx.rate.lbspermmbtu * heat.input.mmbtu, NOx.mass.lbs),
        CO2.mass.tons = if_else(CO2.mass.tons <= 0 | is.na(CO2.mass.tons), CO2.rate.tonspermmbtu * heat.input.mmbtu, CO2.mass.tons)
      ) -> this.file
    
    this.file %>%
      arrange(orispl.code, cems.unit.id, datetime.utc) %>%
      select(
        orispl.code, 
        cems.unit.id,
        datetime.utc, 
        utc.offset.local.time,
        utc.offset.std.time,
        operational.time, 
        gross.load, 
        steam.load, 
        heat.input.mmbtu, 
        everything()
      ) -> this.file
    
    ###############################################
    ###############################################
    ## Hourly Output files
    ###############################################
    ###############################################
    writeLines("Writing Hourly Files")
    ##Always write parquet files
    ##Writing other file types will be depricated
    this.file %>%
      write_parquet(
        file.path(path.hourly.out,glue("{outfile_name}.parquet"))
      )
    
    if("rds" %in% project.local.config$output$formats) {
      writeLines("DEPRICATION WARNING: Writing .rds files from this script will be removed in a future version")
      this.file %>%
        write_rds(
          file.path(path.hourly.out,"rds",glue("{outfile_name}.rds.gz")), 
          compress="gz"
        )
    }

    if("dta" %in% project.local.config$output$formats) {
      writeLines("DEPRICATION WARNING: Writing .dta files from this script will be removed in a future version")
      this.file %>%
        rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
        write_dta(
          file.path(path.hourly.out,"stata",glue("{outfile_name}.dta"))
        )
    }
    
    
    #######################################
    #######################################
    ## Compute daily summaries
    #######################################
    #######################################
    writeLines("Computing Daily Summaries")
    this.file %>%
      mutate(date.utc = as_date(datetime.utc)) %>%
      group_by(orispl.code, cems.unit.id, date.utc) %>%
      summarize(
        elec.load.mwh = sum(gross.load,na.rm=TRUE),
        steam.load.mmbtu = sum(.97*steam.load,na.rm=TRUE),
        total.heat.input.mmbtu = sum(heat.input.mmbtu,na.rm=TRUE),
        elec.heat.input.mmbtu = sum(heat.input.mmbtu - .97*steam.load,na.rm=TRUE),
        operational.time = sum(operational.time,na.rm=TRUE),
        SO2.mass.lbs = sum(SO2.mass.lbs,na.rm=TRUE),
        NOx.mass.lbs = sum(NOx.mass.lbs,na.rm=TRUE),
        CO2.mass.tons = sum(CO2.mass.tons,na.rm=TRUE),
        .groups="drop"
      ) -> this.daily
    
    ##Always write parquet files
    ##Writing other file types will be depricated
    this.daily %>%
      write_parquet(
        file.path(path.daily.out,glue("daily-{outfile_name}.parquet"))
      )
    
    if("rds" %in% project.local.config$output$formats) {
      writeLines("DEPRICATION WARNING: Writing .rds files from this script will be removed in a future version")
      this.daily %>%
        write_rds(
          file.path(path.daily.out,"rds",glue("daily-{outfile_name}.rds.gz")), 
          compress="gz"
        )
    }
    
    if("dta" %in% project.local.config$output$formats) {
      writeLines("DEPRICATION WARNING: Writing .dta files from this script will be removed in a future version")
      this.daily %>%
        rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
        write_dta(
          file.path(path.daily.out,"stata",glue("daily-{outfile_name}.dta"))
        )
    }
    
    ###################
    ##Compute Monthly Summary
    ###################
    writeLines("Computing Monthly Summary")
    this.file %>%
      mutate(
        datetime.local = datetime.utc - hours(utc.offset.local.time),
        month = floor_date(datetime.local,"month")
      ) %>%
      group_by(orispl.code, cems.unit.id, month) %>%
      summarize(
        elec.load.mwh = sum(gross.load,na.rm=TRUE),
        steam.load.mmbtu = sum(.97*steam.load,na.rm=TRUE),
        total.heat.input.mmbtu = sum(heat.input.mmbtu,na.rm=TRUE),
        elec.heat.input.mmbtu = sum(heat.input.mmbtu - .97*steam.load,na.rm=TRUE),
        operational.time = sum(operational.time,na.rm=TRUE),
        SO2.mass.lbs = sum(SO2.mass.lbs,na.rm=TRUE),
        NOx.mass.lbs = sum(NOx.mass.lbs,na.rm=TRUE),
        CO2.mass.tons = sum(CO2.mass.tons,na.rm=TRUE),
        .groups="drop"
      ) -> this.monthly
    
    ##Always write parquet files
    ##Writing other file types will be depricated
    this.monthly %>%
      write_parquet(
        file.path(path.monthly.out,glue("monthly-{outfile_name}.parquet"))
      )
    
    if("rds" %in% project.local.config$output$formats) {
      writeLines("DEPRICATION WARNING: Writing .rds files from this script will be removed in a future version")
      this.monthly %>%
        write_rds(
          file.path(path.monthly.out,"rds",glue("monthly-{outfile_name}.rds.gz")), 
          compress="gz"
        )
    }
    
    if("dta" %in% project.local.config$output$formats) {
      writeLines("DEPRICATION WARNING: Writing .dta files from this script will be removed in a future version")
      this.monthly %>%
        rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
        write_dta(
          file.path(path.monthly.out,"stata",glue("monthly-{outfile_name}.dta"))
        )
    }
    
    this.monthly %>%
      bind_rows(all.monthly.data) -> all.monthly.data
    
    ########################
    ##Update the list of processed files
    ########################
    files.to.process %>%
      mutate(
        processedDatetime = if_else(filename == fi$filename,now(),processedDatetime),
        sourceDatetime.processed = if_else(filename == fi$filename,sourceDatetime,sourceDatetime.processed)
      ) -> files.to.process
    
  } else {
    #Load the monthly data file and append it to all monthly
    read_parquet(
      file.path(path.monthly.out,glue("monthly-{outfile_name}.parquet"))
    ) %>%
      bind_rows(all.monthly.data) -> all.monthly.data
  }
}



####################################
## Write the combined monthly data
####################################
writeLines("Writing all montly data")
##Always write parquet files
##Writing other file types will be deprecated
all.monthly.data %>%
  write_parquet(
    file.path(path.monthly.out,glue("monthly-CEMS-all.parquet"))
  )

if("rds" %in% project.local.config$output$formats) {
  writeLines("DEPRICATION WARNING: Writing .rds files from this script will be removed in a future version")
  all.monthly.data %>%
    write_rds(
      file.path(path.monthly.out,"rds",glue("monthly-CEMS-all.rds.gz")), 
      compress="gz"
    )
}

if("dta" %in% project.local.config$output$formats) {
  writeLines("DEPRICATION WARNING: Writing .dta files from this script will be removed in a future version")
  all.monthly.data %>%
    rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
    write_dta(
      file.path(path.monthly.out,"stata",glue("monthly-CEMS-all.dta"))
    )
}

##################################
## Write the processed file log
##################################]
writeLines("Writing the processed file log")
files.to.process %>%
  select(filename,year,quarter,sourceDatetime.processed,processedDatetime) %>%
  rename(sourceDatetime = sourceDatetime.processed) %>%
  write_csv(path.processed.file.log)



