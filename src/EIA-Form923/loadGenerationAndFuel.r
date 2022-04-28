#DK Spring 2019 - LOAD EIA FUEL GENERATION DATA
  #Original Author: James Archsmith
  #Original Filename: 2_load_eia_923_generation_and_fuel.do
############################################################

rm(list=ls())
library(tidyverse)
library(readxl)     #Excel files
library(lubridate)  #Date and time handling
library(magrittr)   #Better pipes
library(tidyselect) #Better select
library(units)      #Seamless handling of units of measure
library(haven)

g.drive = Sys.getenv("GoogleDrivePath")

#setup
eia923.path <- file.path(g.drive,"Data","Energy","Electricity","EIA","Form EIA-923")
eia923.version <- "20200416"
startYear <- 2001
endYear <- year(today())

#landing pad for data
eia_923_generation_and_fuel <- tibble()

#Where to find data for each year
for(yr in startYear:endYear) {
  #Determine the most recent revision
  list.files(file.path(eia923.path,"data","source",yr),pattern="^[0-9]+$") %>% 
    sort() %>% 
    last() -> year.version
  
  year.path <- file.path(eia923.path,"data","source",yr,year.version)
  
  print(str_c("Source Path: ", year.path))
  flist <- list.files(year.path,pattern="*.[Xx][Ll][Ss]?")
  infile.name = as.character(NA)
  #Determine the type of file we have. It is either "f906920[y\_]<yr>.xls"
  #or ucase matches regex *SCHEDULE*5*
  if(str_c("f906920_",yr,".xls") %in% flist) {
    infile.name = str_c("f906920_",yr,".xls")
    file.format = "EIA906"
  } else if(str_c("f906920y",yr,".xls") %in% flist) {
    infile.name = str_c("f906920y",yr,".xls")
    file.format = "EIA906"
  } else if("eia923December2008.xls" %in%  flist) {
    infile.name = "eia923December2008.xls"
    file.format = "EIA923"
  } else if(any(str_detect(str_to_upper(flist),r"--(SCHEDULE.*[\s\_]2\_3)--"))) {
    candidate.name = flist[str_detect(str_to_upper(flist),r"--(SCHEDULE.*[\s\_]2\_3)--")]
    if(length(infile.name) == 1) {
      infile.name = candidate.name[1]
      file.format = "EIA923"
    }
  } 
  filepath = file.path(year.path,infile.name)
  # if( yr %in% c(2001,2002) ) { filepath = file.path(year.path,paste("f906920y",yr,".xls", sep="")) } 
  # else if( yr %in% c(2003,2004,2005,2006,2007) ) { filepath = file.path(year.path ,paste("f906920_",yr,".xls", sep="")) }
  # else if( yr %in% c(2008) ) { filepath = file.path(year.path,paste("eia923December",yr,".xls", sep="")) }
  # else if( yr %in% c(2009) ) { filepath = file.path(year.path, "EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.xls") }
  # else if( yr %in% c(2010) ) { filepath = file.path(year.path, "EIA923 SCHEDULES 2_3_4_5 Final 2010.xls") }
  # else if( yr %in% c(2011) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_2011_Final_Revision.xlsx") }
  # else if( yr %in% c(2012) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_2012_Final_Release_12.04.2013.xlsx") }
  # else if( yr %in% c(2013) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_2013_Final_Revision.xlsx") }
  # else if( yr %in% c(2014) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_M_12_2014_Final_Revision.xlsx") }
  # else if( yr %in% c(2015) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_M_12_2015_Final_Revision.xlsx") }
  # else if( yr %in% c(2016) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx") }
  # else if( yr %in% c(2017) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_M_12_2017_Final.xlsx") }
  # else if( yr %in% c(2018) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_M_12_2018_Final_Revision.xlsx") }
  # else if( yr %in% c(2019) ) { filepath = file.path(year.path, "EIA923_Schedules_2_3_4_5_M_08_2019_22OCT2019.xlsx")}
  
  #This section works around R naming conventions
    cat("\n")
    print(paste("Importing ",yr,sep=""))
    print(str_c("Filename: ",infile.name))
    #Actual data import: assign read_excel value to an object called "DATA" 
    #if (yr < 2011) { assign("DATA", read_excel(filepath, sheet="Page 1 Generation and Fuel Data", trim_ws=TRUE, skip=7, col_names=TRUE) )}
    #else { assign("DATA", read_excel(filepath, sheet="Page 1 Generation and Fuel Data", trim_ws=TRUE, skip=5, col_names=TRUE) )}
  
    if (yr < 2011) read_excel(filepath, sheet="Page 1 Generation and Fuel Data", trim_ws=TRUE, skip=7, col_names=TRUE) -> DATA
    else read_excel(filepath, sheet="Page 1 Generation and Fuel Data", trim_ws=TRUE, skip=5, col_names=TRUE) -> DATA
    
    #standardize things
    DATA <- rename_all(DATA, toupper) 
    colnames(DATA)  %<>%                      #compound pipe assigns result back to colnames(DATA)
      str_replace_all( r"--(\s)--",  "") %>%
      str_replace_all( r"--([\&\)\(\.])--",  "")
      
    #group (1) renames
    if("MMBTU_PER_UNIT_JAN" %in% colnames(DATA)) {
      print('Renaming: Group (1)')
      colnames(DATA) %<>% 
        str_replace("MMBTU_PER", "MMBTUPER") %>% 
        str_replace("TOT_MMBTU_", "TOT_MMBTU") %>% 
        str_replace("ELEC_MMBTUS_", "ELEC_MMBTU") %>% 
        str_replace("TOTAL FUEL CONSUMPTION MMBTUS", "TOTALFUELCONSUMPTIONMMBTU") %>% 
        str_replace("TOTALFUELCONSUMPTIONMMBTUS", "TOTALFUELCONSUMPTIONMMBTU") %>%  # 
        str_replace("ELEC FUEL CONSUMPTION MMBTUS", "ELECFUELCONSUMPTIONMMBTU") %>%
        str_replace("ELECFUELCONSUMPTIONMMBTUS", "ELECFUELCONSUMPTIONMMBTU") %>% #
        str_replace("TOTAL FUEL CONSUMPTION QUANTITY", "TOTALFUELCONSUMPTIONQUANTITY")
    } else {print("Skipped Group (1) Rename")}
    
    #group (2) renames  
    if("NETGENJANUARY" %in% colnames(DATA)) {
      print('Renaming: Group (2)')
      colnames(DATA) %<>%  
        str_replace("NETGEN", "NETGEN_") %>% 
        str_replace("NETGEN_ERATIONMEGAWATTHOURS", "NETGENERATIONMEGAWATTHOURS") %>% 
        str_replace("MMBTUPER_UNIT", "MMBTUPER_UNIT_") %>% 
        str_replace("ELEC_QUANTITY", "ELEC_QUANTITY_") %>%
        str_replace("QUANTITY", "QUANTITY_") %>%
        str_replace("TOTALFUELCONSUMPTIONQUANTITY_", "TOTALFUELCONSUMPTIONQUANTITY") %>%
        str_replace("ELECTRICFUELCONSUMPTIONQUANTITY_", "ELECTRICFUELCONSUMPTIONQUANTITY")
    } else {print("Skipped Group (2) Rename")}
    
    #group (3) renames (everyone)
    print('Renaming: Group (3)')
    colnames(DATA) %<>%  
      str_replace("PLANTSTATE", "STATE") %>% 
      str_replace("COMBINEDHEATANDPOWERPLANT", "COMBINEDHEATPOWERPLANT") %>% 
      str_replace("JANUARY", "JAN") %>% 
      str_replace("FEBRUARY", "FEB") %>% 
      str_replace("MARCH", "MAR") %>%
      str_replace("APRIL", "APR") %>% 
      str_replace("JUNE", "JUN") %>% 
      str_replace("JULY", "JUL") %>%
      str_replace("AUGUST", "AUG") %>% 
      str_replace("SEPTEMBER", "SEP") %>% 
      str_replace("OCTOBER", "OCT") %>%
      str_replace("NOVEMBER", "NOV") %>% 
      str_replace("DECEMBER", "DEC") %>%
      str_replace_all( "__",  "_")    #sorts out double underscore problems
      
    #destring
    DATA %<>% 
      mutate(NUCLEARUNITID = as.numeric(NUCLEARUNITID)) %>% 
      mutate_at(vars(starts_with("QUANTITY")), as.numeric ) %>%
      mutate_at(vars(starts_with("ELEC")), as.numeric ) %>%
      mutate_at(vars(starts_with("MMBTUPER")), as.numeric ) %>%
      mutate_at(vars(starts_with("NETGEN_")), as.numeric ) %>%
      mutate_at(vars(starts_with("TOT_MMBTU")), as.numeric )
        #I wonder if it is worth "catching" these warnings using tryCatch()?
            #They are all just: "NAs introduced by coercion"
      
    #CLEAN OUT 'NA' COLUMNS
    NA_Cols <- character()
    #first tag them
    for (col in colnames(DATA)) {
      if ( sum(is.na(DATA[[col]] )) == nrow(DATA) ) {
        NA_Cols <- c(NA_Cols, col )
    }}
    # Then drop all at once
    DATA <- select(DATA, -NA_Cols ) 
    cat("Dropped..." , NA_Cols, "... b/c they contain only NA", sep="\n \t")
    
    
    #Append to master data set
    eia_923_generation_and_fuel %<>% bind_rows(DATA)
      #to check individual years...
        #assign(paste("tibble",yr,sep=""), DATA)
}

#Reorder cols
front_cols <- c("PLANTID", "YEAR", 
                "COMBINEDHEATPOWERPLANT", "NUCLEARUNITID", 
                "PLANTNAME", "OPERATORNAME", "OPERATORID", 
                "STATE","CENSUSREGION","NERCREGION", "NAICSCODE", 
                "EIASECTORNUMBER", "SECTORNAME", 
                "REPORTEDPRIMEMOVER","REPORTEDFUELTYPECODE", 
                "AERFUELTYPECODE","PHYSICALUNITLABEL")

eia_923_generation_and_fuel %<>% 
  select(front_cols, everything())



### fancy-pants missing obs. table ###

  #Doing this in 2 steps:
    #1. Find and store the missings
    #2. Make the table

#1.find and store the missings
for(yr in unique(eia_923_generation_and_fuel$YEAR)) {
  YEAR_DATA <- eia_923_generation_and_fuel %>% filter(YEAR == yr)  #Grab a year
  NA_Cols <- character()                                           #reset each time
  
  for (col in colnames(YEAR_DATA)) {
    if ( sum(is.na(YEAR_DATA[[col]]) ) == nrow(YEAR_DATA) ) {      #test if col is full of NAs
      NA_Cols <- c(NA_Cols, col )  }                               #Save names of NA vars  
    assign(paste("missings_",yr,sep=""), NA_Cols)                  #put 'em in a "missings_year" vector
}}

#2. Make the table
for (col in colnames(eia_923_generation_and_fuel)) {
  #catch each row 
  row <- vector()
  for(yr in unique(eia_923_generation_and_fuel$YEAR)) {
    #use "missings_year" vectors saved earlier 
    if (col %in% get(paste("missings_",yr,sep="")) )
      {row <-  append(row, "----" )} else {row <- append(row,yr)}
  }
  cat(row, " ")
  cat("(", col, ")", " ", sep ="")
  cat("\n")
}
# "---- indicates that the var is always missing for a given year"  #


#####################################
## Other data cleaning
#####################################
eia_923_generation_and_fuel %>%
  mutate(
    #Combined heat power plant flag
    is.CHP = case_when(
      COMBINEDHEATPOWERPLANT == "Y" ~ TRUE,
      COMBINEDHEATPOWERPLANT == "N" ~ FALSE,
      TRUE ~ as.logical(NA))
  ) %>%
  select(-COMBINEDHEATPOWERPLANT) -> eia_923_generation_and_fuel





#sort
eia_923_generation_and_fuel %<>% 
  arrange(PLANTID,YEAR)

#OUTPUT to intermediate folder
eia_923_generation_and_fuel %>% 
  rename(
    orispl.code = PLANTID,
    prime.mover = REPORTEDPRIMEMOVER,
    year = YEAR,
    fuel.type = REPORTEDFUELTYPECODE,
    aer.fuel.type = AERFUELTYPECODE,
    fuel.unit = PHYSICALUNITLABEL,
    elec.heat.input = ELECFUELCONSUMPTIONMMBTU,
    elec.fuel.input = ELECTRICFUELCONSUMPTIONQUANTITY,
    net.generation = NETGENERATIONMEGAWATTHOURS,
    total.heat.input = TOTALFUELCONSUMPTIONMMBTU,
    total.fuel.input = TOTALFUELCONSUMPTIONQUANTITY
  ) %>%
  mutate(
    elec.heat.input = set_units(elec.heat.input, "MBTU"),
    total.heat.input = set_units(total.heat.input, "MBTU"),
    net.generation = set_units(net.generation,"MW*h")
  ) %>%
  select(orispl.code,prime.mover,fuel.type,year,everything()) %>%
  arrange(orispl.code,prime.mover,fuel.type,year) %>%
  write_rds(
          file.path(eia923.path,"data","intermediate", "eia_923_generation_and_fuel.rds.gz"),
          compress="gz",compression=2L
          )

#Define a list of variables that should be the unique keys in the output data
EIA923.keys = vars_select(names(eia_923_generation_and_fuel),
                   PLANTID,NUCLEARUNITID,YEAR,REPORTEDPRIMEMOVER,
                   REPORTEDFUELTYPECODE,AERFUELTYPECODE,
                   PHYSICALUNITLABEL)


#Define a list of variable containing data. Each is expected to 
#Have a 3-letter month suffix. We are going to reshape to remove it
EIA923.values = vars_select(names(eia_923_generation_and_fuel), 
                    starts_with("QUANTITY_"),starts_with("ELEC_QUANTITY_"),
                    starts_with("MMPTU_PER_UNIT_"),starts_with("TOT_MMBTU"),
                    starts_with("ELEC_MMBTU"),starts_with("NETGEN_")
                    )

eia_923_generation_and_fuel %>%
  select(all_of(EIA923.keys), all_of(EIA923.values)) %>%
  #Take sums so we have unique key variables
  group_by_at(vars(one_of(EIA923.keys))) %>%
  summarize_at(vars(EIA923.values),sum) %>%
  ungroup() %>%
  #Gateher up all the value columns
  gather(
    EIA923.values,
    key = "KEY",
    value = "VALUE"
  ) %>%
  #Split column names into the name and the 3-letter month
  #Some column names end with an underscore. remove it
  mutate(
    Month = str_sub(KEY,-3,-1),
    KEY = str_sub(KEY,1,-4),
    KEY = str_replace(KEY,"_$","")
  ) %>%
  #Spread the data colums back out
  spread(
    key=KEY,
    value=VALUE
  ) %>%
  #Turn month-year into a date
  mutate(
    Month = ymd(str_c(YEAR,Month,"1",sep="-"))
  ) %>%
  #Remove some unneeded columns
  select(-YEAR) -> EIA923.GenFuel


#Clean up var names
EIA923.GenFuel %<>%
  #Set units on some columns
  mutate(
    ELEC_MMBTU = set_units(ELEC_MMBTU, "MBTU"),
    TOT_MMBTU = set_units(TOT_MMBTU, "MBTU"),
    NETGEN = set_units(NETGEN,"MW*h")
  ) %>%
  rename(
    orispl.code = PLANTID,
    nuclear.unit.id = NUCLEARUNITID,
    prime.mover = REPORTEDPRIMEMOVER,
    fuel.type = REPORTEDFUELTYPECODE,
    aer.fuel.type = AERFUELTYPECODE,
    fuel.unit = PHYSICALUNITLABEL,
    elec.heat.input = ELEC_MMBTU,
    elec.fuel.input = ELEC_QUANTITY,
    net.generation = NETGEN,
    total.heat.input = TOT_MMBTU,
    total.fuel.input = QUANTITY
  ) %>%
  mutate(
    heat.rate.net = elec.heat.input / net.generation
  ) %>%
  select(orispl.code,nuclear.unit.id,prime.mover,fuel.type,Month, everything()) %>%
  arrange(orispl.code,nuclear.unit.id,prime.mover,fuel.type,Month)


#This is just a temporary output file. I'd like to clean it up a lot more before moving on
# write_rds(EIA923.GenFuel, 
#           file.path(eia923.path,"data","intermediate", "eia_923_generation_and_fuel_panel.rds.gz"),
#           compress="gz",compression=4L
# )

path.out <- file.path(eia923.path,"data","out",eia923.version)
if(!dir.exists(path.out)) {
  dir.create(path.out)
  dir.create(file.path(path.out,"R"))
  dir.create(file.path(path.out,"Stata"))
}

EIA923.GenFuel %>%
  write_rds(
    file.path(path.out,"R","EIA923_generation_and_fuel.rds.gz"),
    compress="gz",compression=4L
  ) %>%
  rename_all(list(~str_replace_all(.,r"{[\s\.]}","_"))) %>%
  write_dta(
    file.path(path.out,"Stata","EIA923_generation_and_fuel.dta")
  )
              

