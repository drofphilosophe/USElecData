#DK Spring 2019 - LOAD EIA FUEL GENERATION DATA
  #Original Author: James Archsmith
  #Original Filename: 2_load_eia_923_generation_and_fuel.do
############################################################
library(tidyverse)
library(readxl)     #Excel files
library(lubridate)  #Date and time handling
library(tidyselect) #Better select
library(units)      #Seamless handling of units of measure
library(haven)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EIA-Form923/loadGenerationAndFuel.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

path.source = file.path(path.project, "data","source", "EIA-Form923")

year.start <- as.integer(project.config$sources$`EIA-Form923`$`start-year`)
if(is.null(project.config$sources$`EIA-Form923`$`end-year`)) {
  year.end <- year(today())
} else {
  year.end <- as.integer(project.config$sources$`EIA-Form923`$`end-year`)
}




##An accumulator for loading data
all.data <- tibble()

#Where to find data for each year
for(yr in year.start:year.end) {
  
  year.path <- file.path(path.source,yr)
  if(!dir.exists(year.path)) {
    year.path = file.path(path.source,str_c(yr,"_nonutility"))
    if(!dir.exists(year.path)) {
      writeLines(str_c("Cannot find source data folder for year ",yr,". Skipping."))
      next
    } else {
      writeLines(str_c("Files from ",yr," are in a legacy format that I can't handle. Skipping."))
      next
    }
  }
  
  writeLines(str_c("Source Path: ", year.path))
  
  #List Excel files
  flist <- list.files(year.path,pattern=r"{*.[Xx][Ll][Ss][Xx]?}")
  
  infile.name = as.character(NA)
  #Determine the type of file we have. It is either 
  
  
  if(str_c("f906920_",yr,".xls") %in% flist) {
    #Pattern "f906920_<yr>.xls"
    infile.name = str_c("f906920_",yr,".xls")
    file.format = "EIA906"
  } else if(str_c("f906920y",yr,".xls") %in% flist) {
    #Pattern "f906920y<yr>.xls"
    infile.name = str_c("f906920y",yr,".xls")
    file.format = "EIA906"
  } else if("eia923December2008.xls" %in%  flist) {
    #The odd 2008 filename
    infile.name = "eia923December2008.xls"
    file.format = "EIA923"
  } else if(any(str_detect(str_to_upper(flist),r"{SCHEDULE.*[\s\_]2\_3}"))) {
    #or ucase matches regex *SCHEDULE*5*
    candidate.name = flist[str_detect(str_to_upper(flist),r"{SCHEDULE.*[\s\_]2\_3}")]
    if(length(infile.name) == 1) {
      infile.name = candidate.name[1]
      file.format = "EIA923"
    } else {
      stop(str_c("Multiple files in the ", yr, " folder match the pattern SCHEDULE*5. Aborting."))
    }
  } else if(length(flist) == 0) {
    warning(str_c("No Excel files found in the ", yr, " folder. Skipping."))
    next
  } else {
    stop(str_c("No conforming file name patterns in the ", yr, " folder. Aborting."))
  }
  filepath = file.path(year.path,infile.name)
  
  
  #This section works around R naming conventions
  writeLines("\n")
  writeLines(str_c("Importing ",yr))
  writeLines(str_c("Filename: ",infile.name))
  
  

  if (yr < 2011) {
    read_excel(
      filepath, 
      sheet="Page 1 Generation and Fuel Data", 
      trim_ws=TRUE, 
      skip=7, 
      col_names=TRUE
      ) -> DATA
  } else {
    read_excel(
      filepath, 
      sheet="Page 1 Generation and Fuel Data", 
      trim_ws=TRUE, 
      skip=5, 
      col_names=TRUE
    ) -> DATA
  }
  
  #standardize column names
  DATA %>%
    #Colanmes in upper case
    rename_with(toupper) %>%
    #Remove whitespace
    rename_with(~str_replace_all(.,r"{\s}","")) %>%
    #Remove &, ., parens
    rename_with(~str_replace_all(.,r"{[\&\)\(\.]}","")) -> DATA
      
      
  #group (1) renames
  if("MMBTU_PER_UNIT_JAN" %in% colnames(DATA)) {
    writeLines('Renaming: Group (1)')
    
    DATA %>% 
      rename_with(~str_replace(.,"MMBTU_PER", "MMBTUPER")) %>% 
      rename_with(~str_replace(.,"TOT_MMBTU_", "TOT_MMBTU_")) %>% 
      rename_with(~str_replace(.,"ELEC_MMBTUS_", "ELEC_MMBTU_")) %>% 
      rename_with(~str_replace(.,"TOTAL FUEL CONSUMPTION MMBTUS", "TOTALFUELCONSUMPTIONMMBTU")) %>% 
      rename_with(~str_replace(.,"TOTALFUELCONSUMPTIONMMBTUS", "TOTALFUELCONSUMPTIONMMBTU")) %>% 
      rename_with(~str_replace(.,"ELEC FUEL CONSUMPTION MMBTUS", "ELECFUELCONSUMPTIONMMBTU")) %>%
      rename_with(~str_replace(.,"ELECFUELCONSUMPTIONMMBTUS", "ELECFUELCONSUMPTIONMMBTU")) %>%
      rename_with(~str_replace(.,"TOTAL FUEL CONSUMPTION QUANTITY", "TOTALFUELCONSUMPTIONQUANTITY")) -> DATA
    
  } else {
    writeLines("Skipped Group (1) Rename")
    }
    
  #group (2) renames  
  if("NETGENJANUARY" %in% colnames(DATA)) {
    writeLines('Renaming: Group (2)')
    
    DATA %>%  
      rename_with(~str_replace(.,"NETGEN", "NETGEN_")) %>% 
      rename_with(~str_replace(.,"NETGEN_ERATIONMEGAWATTHOURS", "NETGENERATIONMEGAWATTHOURS")) %>% 
      rename_with(~str_replace(.,"MMBTUPER_UNIT", "MMBTUPER_UNIT_")) %>% 
      rename_with(~str_replace(.,"ELEC_QUANTITY", "ELEC_QUANTITY_")) %>%
      rename_with(~str_replace(.,"QUANTITY", "QUANTITY_")) %>%
      rename_with(~str_replace(.,"TOTALFUELCONSUMPTIONQUANTITY_", "TOTALFUELCONSUMPTIONQUANTITY")) %>%
      rename_with(~str_replace(.,"ELECTRICFUELCONSUMPTIONQUANTITY_", "ELECTRICFUELCONSUMPTIONQUANTITY"))-> DATA
  } else {
    print("Skipped Group (2) Rename")
  }
    
  #group (3) renames (everyone)
  writeLines('Renaming: Group (3)')
  DATA %>%   
    rename_with(~str_replace(.,"PLANTSTATE", "STATE")) %>% 
    rename_with(~str_replace(.,"COMBINEDHEATANDPOWERPLANT", "COMBINEDHEATPOWERPLANT")) %>% 
    rename_with(~str_replace(.,"JANUARY", "JAN")) %>% 
    rename_with(~str_replace(.,"FEBRUARY", "FEB")) %>% 
    rename_with(~str_replace(.,"MARCH", "MAR")) %>%
    rename_with(~str_replace(.,"APRIL", "APR")) %>% 
    rename_with(~str_replace(.,"JUNE", "JUN")) %>% 
    rename_with(~str_replace(.,"JULY", "JUL")) %>%
    rename_with(~str_replace(.,"AUGUST", "AUG")) %>% 
    rename_with(~str_replace(.,"SEPTEMBER", "SEP")) %>% 
    rename_with(~str_replace(.,"OCTOBER", "OCT")) %>%
    rename_with(~str_replace(.,"NOVEMBER", "NOV")) %>% 
    rename_with(~str_replace(.,"DECEMBER", "DEC")) %>%
    rename_with(~str_replace_all(.,"__","_")) -> DATA    #sorts out double underscore problems
  
  
  #There are some cases where there isn't an underscore before the month abriviation
  for(m in month.abb) {
    pat = str_c(r"{([^\_])(}",str_to_upper(m),")$")
    
    DATA %>%
      rename_with(~str_replace(.,pat,r"{\1_\2}")) -> DATA
  }

  #Convert text data to numeric
  DATA %>% 
    mutate(
      across(
        .cols=c(any_of("NUCLEARUNITID"), starts_with("QUANTITY"),starts_with("ELEC"),starts_with("MMBTUPER"),starts_with("NETGEN_"),starts_with("TOT_MMBTU")),
        .fns=as.numeric
      )
    ) -> DATA

  #Remove any columns that are always NA
  writeLines("The following columns contain only missing values and will be dropped")
  DATA %>% select(where(~all(is.na(.)))) %>% names() -> NA_COLS
  for(c in NA_COLS) writeLines(c)
    
  DATA %>%
    select(-any_of(NA_COLS)) -> DATA   

  all.data %>%
    bind_rows(DATA) -> all.data

}

    
    
#Reorder cols
front_cols <- c("PLANTID", "YEAR", 
                "COMBINEDHEATPOWERPLANT", "NUCLEARUNITID", 
                "PLANTNAME", "OPERATORNAME", "OPERATORID", 
                "STATE","CENSUSREGION","NERCREGION", "NAICSCODE", 
                "EIASECTORNUMBER", "SECTORNAME", 
                "REPORTEDPRIMEMOVER","REPORTEDFUELTYPECODE", 
                "AERFUELTYPECODE","PHYSICALUNITLABEL")

all.data %>% 
  select(front_cols, everything()) -> all.data


######################
## Tables of missing observations by year
## this is a diagnostic to make sure we clean 
## up inconsistent variable names across years
######################
writeLines("Years will all missing values for a column marked with an X")

all.data %>% 
  #Categorize columns as being "having at least one nonmissing" (FALSE) or "all NA" (TRUE)
  group_by(YEAR) %>%
  summarize(
    across(.fns=~all(is.na(.)))
  ) %>% 
  #Keep only columns where there is inconsistency. Remember we dropped fully missing columns earlier
  #So a column passing any() is not populated in every year
  select(YEAR,where(any)) %>%
  #This will be hard to read so transpose it to have column names in each row and 
  #Years in the columns
  pivot_longer(
    cols=-YEAR,
    names_to="COLNAME",
    values_to="ALL_NA"
  ) %>%
  mutate(ALL_NA = if_else(ALL_NA,"X","")) %>%
  drop_na(COLNAME,YEAR) %>%
  pivot_wider(
    id_cols=COLNAME,
    names_from=YEAR,
    values_from="ALL_NA"
  ) %>% 
  knitr::kable()



#####################################
## Other data cleaning
#####################################
all.data %>%
  mutate(
    #Combined heat power plant flag
    is.CHP = case_when(
      COMBINEDHEATPOWERPLANT == "Y" ~ TRUE,
      COMBINEDHEATPOWERPLANT == "N" ~ FALSE,
      TRUE ~ as.logical(NA))
  ) %>%
  select(-COMBINEDHEATPOWERPLANT) %>% 
  rename(
    orispl.code = PLANTID,
    prime.mover = REPORTEDPRIMEMOVER,
    year = YEAR,
    nuclear.unit.id = NUCLEARUNITID,
    plant.NERC.region = NERCREGION,
    plant.NAICS = NAICSCODE,
    eia.sector.id = EIASECTORNUMBER,
    fuel.type = REPORTEDFUELTYPECODE,
    aer.fuel.type = AERFUELTYPECODE,
    fuel.unit = PHYSICALUNITLABEL,
    elec.heat.input = ELECFUELCONSUMPTIONMMBTU,
    elec.fuel.input = ELECTRICFUELCONSUMPTIONQUANTITY,
    net.generation = NETGENERATIONMEGAWATTHOURS,
    total.heat.input = TOTALFUELCONSUMPTIONMMBTU,
    total.fuel.input = TOTALFUELCONSUMPTIONQUANTITY,
    ba.code = BALANCINGAUTHORITYCODE,
    report.frequency = RESPONDENTFREQUENCY
  ) %>%
  mutate(
    elec.heat.input = set_units(elec.heat.input, "MBTU"),
    total.heat.input = set_units(total.heat.input, "MBTU"),
    net.generation = set_units(net.generation,"MW*h")
  ) %>%
  select(-any_of(c("OPERATORNAME","PLANTNAME","OPERATORID","STATE","CENSUSREGION","SECTORNAME"))) %>%
  select(orispl.code,prime.mover,fuel.type,year,everything()) %>%
  arrange(orispl.code,prime.mover,fuel.type,year) -> all.data


#Create a long unit-by-month file
all.data %>% 
  pivot_longer(
    cols=ends_with(str_c("_",str_to_upper(month.abb))),
    names_to=c(".value","month"),
    names_pattern=r"{(.+)_([A-Z][A-Z][A-Z])}"
  ) %>%
  mutate(
    month = ymd(str_c(year,month,"01",sep="-"))
  ) %>%
  select(orispl.code,prime.mover,fuel.type,month,everything()) %>%
  select(-year) %>%
  rename(
    fuel.consumed.units = QUANTITY,
    fuel.consumed.elec.units = ELEC_QUANTITY,
    fuel.mmbtu.per.unit = MMBTUPER_UNIT,
    net.elec.generation.MWh = NETGEN,
    fuel.consumed.mmbtu = TOT_MMBTU,
    fuel.consumed.elec.mmbtu = ELEC_MMBTU,
  ) %>%
  mutate(
    heat.rate.elec = fuel.consumed.elec.mmbtu / net.elec.generation.MWh
  ) -> all.data.long




#######################################
#######################################
## Create output files
#######################################
#######################################

#########################
## Create output data folders
#########################
path.out = file.path(path.project,"data","out","EIA-Form923")
dir.create(path.out,recursive=TRUE,showWarnings = FALSE)

########################
## Write RDS files
########################
if("rds" %in% project.local.config$output$formats) {
  dir.create(file.path(path.out,"rds"),recursive=TRUE,showWarnings = FALSE)
  
  all.data.long %>%
    write_rds(
      file.path(path.out,"rds","eia_923_generation_and_fuel_wide.rds.gz"), 
      compress="gz"
    )
}

if("dta" %in% project.local.config$output$formats) {
  dir.create(file.path(path.out,"stata"),recursive=TRUE,showWarnings = FALSE)
  
  all.data.long %>%
    rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
    write_dta(
      file.path(path.out,"stata","eia_923_generation_and_fuel_wide.dta")
    )
}



              

