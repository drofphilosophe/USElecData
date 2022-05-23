############################################################
## loadGenerator.R
############################################################
library(tidyverse)
library(readxl)     #Excel files
library(lubridate)  #Date and time handling
library(tidyselect) #Better select
library(haven)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EIA-Form923/loadGenerator.R")

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



#################################
## Loop through each year of data
#################################
all.data <- tibble()
for(yr in year.start:year.end) {
  print(str_c("Processing ", yr))
  
  infile.path <- file.path(path.source,yr)

  if(dir.exists(infile.path)) {
    writeLines(str_c("Source Path: ", infile.path))  
  } else {
    writeLines(str_c("Could not find a source data folder for year ", yr,". Skipping."))
    next
  }
  

  #Get a list of Excel files
  flist <- list.files(infile.path,pattern=r"{*.[Xx][Ll][Ss][Xx]?}")
  
  #exclude any temporary files created by excel. They have a tilde at the start of the filename
  flist = flist[!str_detect(flist,"^~")]
  
  infile.name = as.character(NA)
  
  #Determine the type of file we have. It is either "f906920[y\_]<yr>.xls"
  #or ucase matches regex *SCHEDULE*5*
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
  filepath = file.path(infile.path,infile.name)
  
  
  #This section works around R naming conventions
  writeLines("\n")
  writeLines(str_c("Importing ",yr))
  writeLines(str_c("Filename: ",infile.name))
  

  #Determine which sheet to load
  sheet.list <- excel_sheets(file.path(infile.path,infile.name))
  sheet.list.1 <- sheet.list[str_detect(str_to_upper(sheet.list),"GENERATOR")]
  if(length(sheet.list.1 == 1)) {
    sheet.name = sheet.list.1[1]
  } else {
    writeLines("Cannot determine which sheet contains generator data")
    if(yr<2008) {
      writeLines("This is expected. The data don't exist prior to 2008. Skipping")
      next
    } else {
      writeLines("The file contains the following sheet names:")
      for(s in sheet.list) {
        writeLines(s)
      }
      stop("Aborting") 
    }
  }
  
  ##Deterine the first row of the data table. I do this by reading the first 20 rows of column 1
  writeLines(str_c("Loading sheet ", sheet.name))
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
    mutate(
      across(.cols=contains("Month"),.fns=as.double),
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
  mutate(
    Month = as.integer(Month),
    Month = ymd(str_c(year,Month,"01",sep="-"))
    ) %>%
  select(
    orispl.code,eia.generator.id, Month, -year, everything()
  ) %>%
  rename(
    net.generation.mwh = `NET GENERATION`
  ) %>%
  arrange(orispl.code,eia.generator.id, Month) -> all.data



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
  
  all.data %>%
    write_rds(
      file.path(path.out,"rds","eia_923_generator.rds.gz"), 
      compress="gz"
    )
}

if("dta" %in% project.local.config$output$formats) {
  dir.create(file.path(path.out,"stata"),recursive=TRUE,showWarnings = FALSE)
  
  all.data %>%
    rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
    write_dta(
      file.path(path.out,"stata","eia_923_generator.dta")
    )
}






