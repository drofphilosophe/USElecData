rm(list=ls())
library(tidyverse)
library(lubridate)
library(haven)
library(readxl)

g.drive = Sys.getenv("GoogleDrivePath")
eia860.path <- file.path(g.drive,"Data", "Energy","Electricity", "EIA", "Form EIA-860")

startYear <- 2018
endYear <- 2018

############################################
## Define functions to read the various formats of data
## and return the results as a tibble with conformed
## variable names
##  Functions should take a path to the source data folder
##  and a year
############################################

##2017+ Format
eia860.read.generator.2017 <- function(data.path, year) {
  #Construct the filename 
  path <- file.path(data.path,"data","source",year,str_c("3_1_Generator_Y",year,".xlsx"))
  
  read_excel(path, sheet="Operable", skip=1) %>%
    mutate(generator.status = "Operating") ->  data.operable
  
  read_excel(path, sheet="Proposed", skip=1) %>%
    mutate(generator.status = "Proposed") -> data.proposed
  
  read_excel(path, sheet="Retired and Canceled", skip=1) %>%
    mutate(generator.status = "Retired/Canceled") -> data.retired
  
  #Bind tables
  #data.operable %>% 
  #  bind_rows(data.proposed) %>% 
  #  bind_rows(data.retired) -> data
  
  #############################################
  ## Make a datafile of dates plants were retired
  #############################################
  data.operable %>%
    mutate(orispl.code = as.integer(`Plant Code`)) %>%
    distinct(orispl.code) %>%
    mutate(
      is.operating = TRUE,
      date.retired = as.Date(NA)
    ) -> summary.operable
  
  data.retired %>%
    mutate(orispl.code = as.integer(`Plant Code`)) %>%
    mutate(date.retired = ymd(str_c(`Retirement Year`,`Retirement Month`,"1", sep="-"))) %>%
    group_by(orispl.code) %>%
    summarize(
      date.retired = max(date.retired)
    ) %>%
    mutate(
      is.operating = FALSE
    ) -> summary.retired
  
  summary.operable %>%
    full_join(summary.retired,by="orispl.code", suffix=c(".op",".ret")) %>%
    mutate(
      plant.operating = case_when(
        is.na(is.operating.op) & is.operating.ret == FALSE ~ FALSE,
        is.operating.op == TRUE ~ TRUE,
        TRUE ~ as.logical(NA)
      ),
      date.retired = case_when(
        plant.operating ~ as.Date(NA),
        !plant.operating ~ date.retired.ret
      )
    ) %>%
    mutate(as.integer(orispl.code)) %>%
    arrange(orispl.code) %>%
    select(orispl.code, plant.operating, date.retired) -> summary.plants
    
  summary.plants %>%
    rename_all(.funs=list(~str_replace_all(.,"\\.","_"))) %>%
    write_dta(file.path(data.path,"data","intermediate",str_c("operating_status_by_plant.dta")))
  
  data.operable %>%
    mutate(orispl.code = as.integer(`Plant Code`)) %>%
    mutate(coal = `Energy Source 1` %in% c("BIT","LIG","SUB","RC")) %>%
    group_by(orispl.code) %>%
    summarize(
      total.capacity = sum(`Nameplate Capacity (MW)`),
      coal = any(coal)
    ) %>%
    filter(total.capacity <= 25 & coal == TRUE) %>%
    rename_all(.funs=list(~str_replace_all(.,"\\.","_"))) %>%
    write_dta(file.path(data.path,"data","intermediate",str_c("small_coal_plants.dta")))
    
    
  
  #glimpse(data)
  
  #Conform column names
  data %>%
    rename(
      eia860.utility.id = `Utility ID`,
      eia860.utility.name = `Utility Name`,
      orispl.code = `Plant Code`,
      plant.name = `Plant Name`,
      state.abriv = `State`,
      county.name = `County`,
      eia.generator.id = `Generator ID`,
      generator.technology = `Technology`,
      prime.mover = `Prime Mover`,
      eia.unit.id = `Unit Code`,
      ownership = `Ownership`,
      has.duct.burners = `Duct Burners`,
      bypass.heat.recovery = `Can Bypass Heat Recovery Steam Generator?`
    ) -> data

  
  
  
  #Check assumptions that will be the basis of future mutate() commands
  data %>%
    assert( in_set("","J","S","W","U","N"), ownership) %>%
    assert( in_set("","X","Y","N"), has.duct.burners ) %>%
    assert( in_set("","X","Y","N"), bypass.heat.recovery)

  
  
    
  #Conform data in remaining columns
  data %>%
    mutate(
      ownership = parse_factor(ownership,levels=c("J","S","W","U","N"), na=c("")),
      has.duct.burners = case_when(
        has.duct.burners == "X" ~ as.logical(NA),
        has.duct.burners == "" ~ as.logical(NA),
        has.duct.burners == "Y" ~ TRUE,
        has.duct.burners == "N" ~ FALSE
      ),
      bypass.heat.recovery = case_when(
        bypass.heat.recovery == "X" ~ as.logical(NA),
        bypass.heat.recovery == "" ~ as.logical(NA),
        bypass.heat.recovery == "Y" ~ TRUE,
        bypass.heat.recovery == "N" ~ FALSE        
      )
    ) -> data
    
  return(data)
}






##Loop through each year of data
for(yr in startYear:endYear) {
  print(paste("Processing", yr))
  
  if(yr >= 2017) data <- eia860.read.generator.2017(eia860.path,yr)
  #Raise an error if we try to process a year for which 
  #we don't have a handler
  else {
    stop(paste("I don't know how to process year",yr))
  }
  
  #What I really want right now is a mapping of orispl.code and eia.generator.id to eia.unit.id
  data %>%
    distinct(orispl.code, eia.generator.id, eia.unit.id) %>%
    mutate(
      eia.unit.id = case_when(
        eia.unit.id != "" ~ eia.unit.id,
        TRUE ~ as.character(NA) 
      ),
      year = yr
    ) %>%
    arrange(year, orispl.code, eia.generator.id) -> generator.unit.map
  
  generator.unit.map %>%
    write_rds(
      file.path(eia860.path,"data","intermediate","generatorID_unitID_map.rds.gz"),
      compress="gz"
    )
  
}