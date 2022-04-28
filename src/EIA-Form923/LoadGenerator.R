############################################################
## load_eia923_generator_data.R
##
## Loads generator data from EIA-923.
##
## This departs from my previous code base by programatically
## determining the excel file to load for each year. I operate
## under the assumption that each year contains either:
##  1 - No subfolders (for historical data that were final when I downloaded them)
##  2 - Subfolders named YYYYMMDD for the date that version of the data were downloaded
##
## Things like sheet names and column specs I will still need to deal with manually.
##
##  20200417 - File Created
############################################################

rm(list=ls())
library(tidyverse)
library(readxl)     #Excel files
library(lubridate)  #Date and time handling
library(magrittr)   #Better pipes
library(tidyselect) #Better select
library(units)      #Seamless handling of units of measure

#setup
eia923.path <- file.path("G:","My Drive","Data","Energy","Electricity","EIA","Form EIA-923")
eia923.version <- "20200416"
startYear <- 2008 # EIA began reporting boiler fuel data in 2008
endYear <- 2019

all.data <- tibble()
for(yr in startYear:endYear) {
  print(str_c("Processing ", yr))
  year.path <- file.path(eia923.path,"data","source",yr)
  ###########
  ## check for YYYYMMDD subfolders
  ##########
  dir.list <- sort(list.dirs(year.path, full.names=FALSE),decreasing=TRUE)
  if(length(dir.list) > 0) {
    infile.path <- file.path(year.path,dir.list[1])
  } else {
    infile.path <- year.path
  }
  print(str_c("Source Path: ", infile.path))
  flist <- list.files(infile.path,pattern="*.xls?")
  infile.name = as.character(NA)
  #Determine the type of file we have. It is either "f906920[y\_]<yr>.xls"
  #or ucase matches regex *SCHEDULE*5*
  if(str_c("f906920_",yr,".xls") %in% flist) {
    infile.name = str_c("f906920_",yr,".xls")
    file.format = "EIA906"
  } else   if(str_c("f906920y",yr,".xls") %in% flist) {
    infile.name = str_c("f906920y",yr,".xls")
    file.format = "EIA906"
  } else if(any(str_detect(str_to_upper(flist),"SCHEDULE.*[\\s\\_]5"))) {
    candidate.name = flist[str_detect(str_to_upper(flist),"SCHEDULE.*[\\s\\_]5")]
    if(length(infile.name) == 1) {
      infile.name = candidate.name[1]
      file.format = "EIA923"
    }
  } 
  
  if(is.na(infile.name)) {
    print("No idea which file contains boiler fuel data")
    print("The file list follows:")
    for(f in flist) {
      print(f)
    }
    stop("Aborting")
  }
  
  print(str_c("Loading ", infile.name))
  #Determine which sheet to load
  sheet.list <- excel_sheets(file.path(infile.path,infile.name))
  sheet.list.1 <- sheet.list[str_detect(str_to_upper(sheet.list),"GENERATOR")]
  if(length(sheet.list.1 == 1)) {
    sheet.name = sheet.list.1[1]
  } else {
    print("Cannot determine which sheet contains generator data")
    for(s in sheet.list) {
      print(s)
    }
    stop("Aborting")
  }
  
  ##Deterine the first row of the data table. I do this by reading the first 20 rows of column 1
  print(str_c("Loading sheet ", sheet.name))
  read_excel(
    file.path(infile.path,infile.name),
    sheet=sheet.name,
    range="A1:A20",
    col_names="Col1"
  ) %>%
    mutate(rownum = row_number()) %>%
    filter(str_to_upper(Col1) == "PLANT ID") %>%
    pull(rownum) -> toprow
  
  read_excel(
    file.path(infile.path,infile.name),
    sheet=sheet.name,
    skip = toprow[1]-1
  ) %>%
    rename_all(.funs=list(~str_to_upper(.))) %>%
    rename_all(.funs=list(~str_squish(str_replace_all(.,"[\\r\\n]"," ")))) %>%
    rename_at(vars(contains("PRIME MOVER")),list(~"REPORTED PRIME MOVER")) %>%
    rename(
      orispl.code = `PLANT ID`,
      eia.generator.id = `GENERATOR ID`,
      prime.mover = `REPORTED PRIME MOVER`
    ) -> this.year
  
  for(i in 1:length(month.name)) {
    this.year %>%
      rename_all(
        .funs=list(~str_replace(., str_to_upper(month.name[i]), str_c("Month",i) ))
      ) -> this.year
  }
  
  this.year %>%
    select(
      orispl.code,eia.generator.id,prime.mover,
      contains("Month")
    ) %>% 
    #All "month" terms should be numeric
    mutate_at(
      vars(contains("Month")),
      as.double
    ) %>%
    mutate(
      year = yr
    ) %>%
    bind_rows(all.data) -> all.data
}

all.data %>%
  pivot_longer(
    contains("Month"),
    names_to = c(".value","Month"),
    names_sep = " Month"
  ) %>%
  mutate(Month = as.integer(Month),
         Month = ymd(str_c(year,Month,"1",sep="-"))) %>%
  select(
    orispl.code,eia.generator.id, Month, -year, everything()
  ) %>%
  rename(
    net.generation.mwh = `NET GENERATION`
  ) %>%
  arrange(orispl.code,eia.generator.id, Month) -> all.data

if(!dir.exists(file.path(eia923.path,"data","out",eia923.version))) {
  dir.create(file.path(eia923.path,"data","out",eia923.version))
  dir.create(file.path(eia923.path,"data","out",eia923.version,"R"))
  dir.create(file.path(eia923.path,"data","out",eia923.version,"Stata"))
}

all.data %>%
  write_rds(
    file.path(eia923.path,"data","out",eia923.version,"R","EIA923_Generator.rds.gz"), 
    compress="gz"
  ) %>%
  rename_all(
    .funs=list(~str_replace_all(.,"[\\.\\s]","_"))
  ) %>%
  write_dta(
    file.path(eia923.path,"data","out",eia923.version,"Stata","EIA923_Generator.dta")
  )



