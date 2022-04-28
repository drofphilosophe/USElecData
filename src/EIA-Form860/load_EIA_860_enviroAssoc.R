rm(list=ls())
library(tidyverse)
library(lubridate)
library(readxl)
library(assertr)

g.drive = Sys.getenv("GoogleDrivePath")

eia860.path <- file.path(g.drive,"Data", "Energy","Electricity", "EIA", "Form EIA-860")

startYear <- 2017
endYear <- 2017

############################################
## Define functions to read the various formats of data
## and return the results as a tibble with conformed
## variable names
##  Functions should take a path to the source data folder
##  and a year
############################################

##2017+ Format
eia860.read.enviro.boiler.generator.2017 <- function(data.path, year) {
  #Construct the filename 
  path <- file.path(data.path,"data","source",yr,str_c("6_1_EnviroAssoc_Y",yr,".xlsx"))
  
  read_excel(path, sheet="Boiler Generator", skip=1) -> data.boiler.generator
  

  ##All I care about right now is making a mapping of boiler IDs to GeneratorIDs
  ##Sorry whoever works on this in the future
  
  data.boiler.generator %>%
    rename(
      orispl.code = `Plant Code`,
      eia.boiler.id = `Boiler ID`,
      eia.generator.id = `Generator ID`
    ) %>%
    distinct(orispl.code, eia.boiler.id, eia.generator.id) %>%
    filter(!is.na(orispl.code)) -> data.boiler.map
  
  glimpse(data.boiler.map)

  return(data.boiler.map)
}






##Loop through each year of data
for(yr in startYear:endYear) {
  print(paste("Processing", yr))
  
  if(yr >= 2017) data <- eia860.read.enviro.boiler.generator.2017(eia860.path,yr)
  #Raise an error if we try to process a year for which 
  #we don't have a handler
  else {
    stop(paste("I don't know how to process year",yr))
  }
  
  #Take eia.generator.id and make a generator.id to generator.id mapping
  data %>%
    distinct(orispl.code, eia.generator.id) %>%
    mutate(
      eia.boiler.id = eia.generator.id
    ) %>%
    bind_rows(data) %>%
    rename(cems.unit.id = eia.boiler.id) %>%

    distinct(orispl.code, cems.unit.id, eia.generator.id) %>%
    arrange(orispl.code, cems.unit.id, eia.generator.id)-> data.boiler.map
  
  #Merge in eia.unit.id from the generator table
  generator.unit.map <-
    read_rds(
      file.path(eia860.path,"data","intermediate","generatorID_unitID_map.rds.gz")
    )
  
  data.boiler.map %>%
    full_join(
      generator.unit.map,
      by=c("orispl.code","eia.generator.id")
    ) %>%     
    mutate(
      cems.unit.id = case_when(
        is.na(cems.unit.id) ~ eia.generator.id,
        TRUE ~ cems.unit.id
      )
    ) -> big.map
  
  big.map %>%
    distinct(orispl.code, cems.unit.id, eia.unit.id) -> cems.unit.to.eia.unit
  
  big.map %>%
    distinct(orispl.code, eia.generator.id, eia.unit.id) -> eia.generator.to.eia.unit
  
  
}