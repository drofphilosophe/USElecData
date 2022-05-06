rm(list=ls())
library(tidyverse)
library(tcltk)  #Provides a platform-independent progress bar
library(lubridate)
library(sf)
library(units)
library(haven)
library(magrittr)
library(here)
library(yaml)

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
folder.list <- list.dirs(path.hourly.source, full.names = FALSE, recursive=FALSE)

#Load the file log and compile a list of source files
#And their last downloaded times
read_csv(
  file.path(path.hourly.source,"SourceFileLog.csv")
) %>%
  rename(
    Name = filename,
    mtime = download_timestamp
    ) %>%
  mutate(
    Month = as.integer(str_sub(Name,7,8)),
    State = str_sub(Name,5,6),
    Quarter = ceiling(Month/3)
  ) -> input.file.log

#Compile a list of processed hourly files
file.list = list.files(file.path(path.hourly.out,"rds"),pattern=".gz")
file.times = file.info(file.path(path.hourly.out,"rds",file.list))$mtime

tibble(
  Name = file.list,
  mtime = ymd_hms(file.times)
) %>%
  mutate(
    Year = as.integer(str_sub(Name,6,9)),
    Quarter = as.integer(str_sub(Name,11,11)),
  ) -> output.file.log


input.file.log %>%
  group_by(Year,Quarter) %>%
  summarize(
    last.download = max(mtime)
  ) %>%
  full_join(output.file.log,by=c("Year","Quarter")) %>%
  rename(last.update = mtime) %>%
  replace_na(list(last.download=as_datetime(Inf),last.update=as_datetime(-Inf))) %>%
  drop_na() %>%
  filter(last.download > last.update) %>%
  select(Year,Quarter) -> pending.updates

writeLines("Data to be Updated")
knitr::kable(pending.updates)




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

#Define a list of column names. There are two data formats in CEMS files
#The 24-column format contains "FAC_ID" and "UNIT_ID" columns not in the 
#22-column format. These are just ID numbers and don't provide additional 
#information. Further, they don't map to any external dataset.
column.names.24 = c(
  "state",
  "plant.name.cems",
  "orispl.code",
  "cems.unit.id",
  "OP_DATE",
  "OP_HOUR",
  "operational.time",
  "gross.load",
  "steam.load",
  "SO2.mass.lbs",
  "SO2.mass.measure.flag",
  "SO2.rate.lbspermmbtu",
  "SO2.rate.measure.flag",
  "NOx.rate.lbspermmbtu",
  "NOx.rate.measure.flag",
  "NOx.mass.lbs",
  "NOx.mass.measure.flag",
  "CO2.mass.tons",
  "CO2.mass.measure.flag",
  "CO2.rate.tonspermmbtu",
  "CO2.rate.measure.flag",
  "heat.input.mmbtu",
  "FAC_ID",
  "UNIT_ID"
)
#The 22-column format is the same as the 24 less the last two columns
column.names.22 = column.names.24[1:22]

#Define data types for each column. Being explicit speeds loading
#reduces import errors, and actually makes the following code cleaner.
column.spec.24 = cols(
  state = col_character(),
  plant.name.cems = col_character(),
  orispl.code = col_integer(),
  cems.unit.id = col_character(),
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
  FAC_ID = col_skip(),
  UNIT_ID = col_skip()
)
column.spec.22 = cols(
  state = col_character(),
  plant.name.cems = col_character(),
  orispl.code = col_integer(),
  cems.unit.id = col_character(),
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
  heat.input.mmbtu = col_double()
)

#Load orispl.code to time zone mapptings into a data frame
#we'll use them to set time zones later
read_rds(file.path(path.facility.intermediate,"CEMS_Facility_time_zones.rds")) %>%
  mutate(tz.name = as.character(tz.name)) %>%
  select(orispl.code,tz.name) %>%
  arrange(orispl.code) -> tz.map

#Make a list of facilities that have time zone information
tz.map %>%
  select(orispl.code) %>%
  mutate(has.tz = TRUE) -> orispl.with.tz

read_rds(
  file.path(path.facility.out,"rds", "CEMS_Facility_Attributes.rds.bz2")
  ) %>% 
  distinct(orispl.code, plant.name.cems,year) -> facility.names



#Loop through each year from the start to the end
#We're going to write one output file per year
for(yr in year(date.start):year(date.end) ) {
  for(q in 1:4) {
    
    if(date.start > ymd(str_c(yr,(q-1)*3+1,1,sep="-")) ) next
    if(date.end < ymd(str_c(yr,q*3,1,sep="-"))) next
    
    #Do we need to process this quarter?
    #Yes if the mtime on the output file is before the mtime 
    #On any source file for this quarter
    input.file.log %>%
      filter(Year == yr & Quarter == q) %>%
      summarize(mtime = max(mtime)) %>%
      pull(mtime) -> input.max.time
    
    #If there are no responsive files, input.max.time will be -Inf
    if(input.max.time == -Inf) {
      writeLines(str_c("No nput files for ",yr,"Q",q,". Skipping."))
      next
    }
    
    output.file.log %>%
      filter(Year == yr & Quarter == q) %>%
      summarize(mtime = min(mtime)) %>%
      pull(mtime) -> output.min.time
    
    #Check to see if we have an output file
    #output.min.time will be Inf if there are no responsive files
    #We've made it this far, so if no output file exists, we need to make one
    if(output.min.time < Inf) {
      #Compare the largest input file time to the smallest output file time
      if(input.max.time < output.min.time) {
        writeLines(str_c("Input files for ",yr,"Q",q," are all older than the output file."))
        writeLines("It is up-to-date. Skipping")
        next
      }
    }
    

    
    #Print out where we are
    print(paste("Processing",yr))
    
    #Make a list of facility names for this year
    facility.names %>%
      filter(year == yr) %>%
      group_by(plant.name.cems) %>%
      summarize(
        orispl.code = max(orispl.code),
        n = n(),
        .groups="drop"
      ) %>%
      filter(n == 1) %>%
      ungroup() %>%
      select(-n) -> facility.names.current.year
    
    #init an empty tibble to hold the full year of data
    this.year <- tibble()
    
    #Check to see if a folder for this year exists
    #If not, we'll warn the user
    if(as.character(yr) %in% folder.list) {
      #Loop through every month in the quarter
      for(m in ((q-1)*3+1):(q*3) ) {
        #Get a list of files in this month.
        input.file.log %>%
          filter(Year == yr & Month == m) %>%
          pull(Name) -> file.list
        
        if(length(file.list)==0) {
          print(str_c("No files found for year ",yr," month ", m, ". Skipping."))
          next
        }
        

        #init a tibble to hold information about the file we've processed for this year
        files.loaded = tibble()
        
        #init a progress bar
        progress.bar <- tkProgressBar(title = paste("Processing files from",yr,"M",m), 
                                      min = 0,
                                      max = length(file.list), 
                                      width = 500)
        progress.counter <- 0
        
        #Loop through all the files
        for(f in file.list) {
          print(paste("Reading",f))
          
          
          #Inspect the first row of f to determine the number of columns
          #I tell it to skip field names and read everything as text to 
          #speed it up and reduce the verbosity of output
          read_csv(
            file.path(path.hourly.source,yr,f), 
            n_max=1,
            col_types=cols(
              .default=col_character()),
            col_names=FALSE) %>% 
            length() -> column.count
          
          
          
          #If we have 22 columns use the 22-column format
          #Otherwise the 24 column format
          if(column.count == 22) {
            column.names = column.names.22
            column.spec = column.spec.22
          } else {
            column.names = column.names.24
            column.spec = column.spec.24
          }
          
          
          #Read the compressed CSV. Skip the first row since I provided column names
          read_csv(
            file.path(path.hourly.source,yr,f), 
            col_names=column.names, 
            col_types=column.spec, 
            skip=1
          ) -> data.raw
          
          #Count the observations we loaded
          data.raw %>% 
            group_by() %>% 
            tally() %>% 
            as.numeric -> obs.count
          
          #Only continue processing if we read in a non-zero 
          #number of observations
          if(obs.count > 0) {
            data.raw %<>%
              #Sort order will be important later when we fix
              #DST errors. Create a record of the original order
              #in the data.
              #Convert NA values for steam.load to zeros
              mutate(
                row.id = row_number(),
                steam.load = ifelse(is.na(steam.load),0,steam.load)
              ) %>%
              filter(!(orispl.code %in% bad.orispl.list)) -> data.raw
            

            ## There are cases where the wrong ORISPL code is reported for a 
            ## plant. We're going to match the ORISPL to our cannonical list
            ## or ORISPL codes then deal with plants that don't match
            
            ##Join cannonical ORISPL list
            data.raw %>%
              left_join(orispl.with.tz, by="orispl.code") -> data.raw
            
            #Look for ORISPL codes that didn't match
            data.raw %>%
              filter(is.na(has.tz)) %>% 
              group_by(orispl.code) %>% 
              summarize(n=n(),.groups="drop") -> bad.orispl
            
            #Only do the following crap if we need to fix ORISPL codes
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
              if( count(orispl.update) > 0) {
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
              
              if(count(bad.orispl) > 0) {
                data.raw %>%
                  mutate(
                    orispl.code.new = case_when( 
                      #Colorado, Brush 2/4
                      yr >= 2005 & orispl.code == 55209 ~ 10682L, 
                      #Default to the original ORISPL code
                      TRUE ~ as.integer(NA)
                    )
                  ) -> data.raw        
                
                
                if(yr==2005) {
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
                } else if(yr==2006) {
                  # No exceptions       
                } else if(yr==2007) {
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
            
            
            
            ###################################################
            ## Deal with time zones
            ## All times in CEMS are reported in local standard time
            ## Since some places change time zones over time, I've mapped
            ## each plant to an Olson name and I use these to compute the 
            ## local time offset. It's a little tricky because lubridate
            ## will want to account for DST when using local times. I get 
            ## around this by computing the local STANDARD time offset
            ## on each day at 12 AM, converting OP_HOUR and OP_DATE to 
            ## a UTC time then subtracting the offset.
            ##################################################
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
                  datetime.lst = mdy_h(str_c(OP_DATE,OP_HOUR,sep=" "), tz=t),
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
            #Append to previous data
            this.file %>% 
              bind_rows(this.year) -> this.year
            
            rm(list=c("this.file"))
          }
        
          #Keep a record of the files we processed
          tibble(
            state = toupper(substr(f,5,6)),
            year = as.integer(substr(f,1,4)),
            month = as.integer(substr(f,7,8)),
            obs.count = obs.count
          ) %>% 
            bind_rows(files.loaded) -> files.loaded
          
          #Clean up. I do this for error checking
          rm(list=c("data.raw"))
          
          #Update the progress bar
          progress.counter<-progress.counter+1
          setTkProgressBar(progress.bar, 
                           progress.counter, 
                           label=paste(progress.counter,"of",length(file.list),"files"))
        }
        #Close the progress bar
        close(progress.bar)
        
        this.year %>%
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
          ) -> this.year
      }
      #We've finished the year. Write it to a file. 
      #Just going to used compressed RDS for now
      if(exists("this.year")) {
        if("rds" %in% project.local.config$output$formats) {
          this.year %>%
            write_rds(
              file.path(path.hourly.out,"rds",str_c("CEMS_",yr,"Q",q,".rds.gz")), 
              compress="gz"
              )
        }
        
        if("dta" %in% project.local.config$output$formats) {
          this.year %>%
            rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
            write_dta(
              file.path(path.hourly.out,"stata",str_c("CEMS_",yr,"Q",q,".dta"))
              )
        }
      }
      
      
      
      ###################
      ## Compute daily summaries
      ###################
      this.year %>%
        mutate(date.utc = as_date(datetime.utc)) %>%
        group_by(orispl.code, cems.unit.id, date.utc) %>%
        summarize(
          elec.load.mwh = sum(gross.load),
          steam.load.mmbtu = sum(.97*steam.load),
          total.heat.input.mmbtu = sum(heat.input.mmbtu),
          elec.heat.input.mmbtu = sum(heat.input.mmbtu - .97*steam.load),
          operational.time = sum(operational.time),
          SO2.mass.lbs = sum(SO2.mass.lbs),
          NOx.mass.lbs = sum(NOx.mass.lbs),
          CO2.mass.tons = sum(CO2.mass.tons),
          .groups="drop"
        ) -> this.daily
      
      if(exists("this.daily")) {
        if("rds" %in% project.local.config$output$formats) {
          this.daily %>%
            write_rds(
              file.path(path.daily.out,"rds",paste0("CEMS_",yr,"Q",q,".rds.gz")), 
              compress="gz"
            )
        }
          
        if("dta" %in% project.local.config$output$formats) {  
          this.daily %>%
            rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
            write_dta(
              file.path(path.daily.out,"stata",paste0("CEMS_",yr,"Q",q,".dta"))
              )
        }
      }      
      
      ###################
      ##Compute Monthly Summary
      ###################
      this.year %>%
        mutate(
          datetime.local = datetime.utc - hours(utc.offset.local.time),
          month = floor_date(datetime.local,"month")
        ) %>%
        group_by(orispl.code, cems.unit.id, month) %>%
        summarize(
          elec.load.mwh = sum(gross.load),
          steam.load.mmbtu = sum(.97*steam.load),
          total.heat.input.mmbtu = sum(heat.input.mmbtu),
          elec.heat.input.mmbtu = sum(heat.input.mmbtu - .97*steam.load),
          operational.time = sum(operational.time),
          SO2.mass.lbs = sum(SO2.mass.lbs),
          NOx.mass.lbs = sum(NOx.mass.lbs),
          CO2.mass.tons = sum(CO2.mass.tons),
          .groups="drop"
        ) -> this.monthly
      
      if(exists("this.monthly")) {
        if("rds" %in% project.local.config$output$formats) {
          this.monthly %>%
            write_rds(
              file.path(path.monthly.out,"rds",paste0("CEMS_",yr,"Q",q,".rds.gz")), 
              compress="gz"
            )
        }
        
        if("dta" %in% project.local.config$output$formats) {  
          this.monthly %>%
            rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
            write_dta(
              file.path(path.monthly.out,"stata",paste0("CEMS_",yr,"Q",q,".dta"))
            )
        }
      } 
      
      
    } else {
      warning(paste("No source data for year", yr,"quarter",q))
    }
  }
}



####################################
## Combine all monthly data into a single file
###################################
file.list = list.files(file.path(path.monthly.out,"rds"),pattern=".gz")
#Remove the "all months" file from the list of files to combine
#Otherwise, we'd get an ouroboros after multiple runs 
file.list = setdiff(file.list,"CEMS_All_months.rds.gz")

all.months <- tibble()
for(f in file.list) {
  read_rds(file.path(path.monthly.out,"rds",f)) %>%
    bind_rows(all.months) -> all.months
}

if("rds" %in% project.local.config$output$formats) {
  all.months %>%
    write_rds(
      file.path(path.monthly.out,"rds","CEMS_All_Months.rds.gz"), 
      compress="gz"
    )
}

if("dta" %in% project.local.config$output$formats) {  
  all.months %>%
    rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
    write_dta(
      file.path(path.monthly.out,"stata","CEMS_All_Months.dta")
    )
}





