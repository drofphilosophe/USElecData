#DK Spring 2019 - LOAD EIA FUEL PURCHASE DATA
  #Original Author: James Archsmith
  #Original Filename: 3_load_eia_923_fuel_purchase.do
############################################################

rm(list=ls())
library(tidyverse)
library(readxl)
library(lubridate)
library(magrittr)
library(haven)
library(haven)

g.drive = Sys.getenv("GoogleDrivePath")

#setup
eia923.path <- file.path(g.drive,"Data","Energy","Electricity","EIA","Form EIA-923")
eia923.version <- "20200416"

startYear <- 2008
endYear <- year(today())

#landing pad for data
eia_923_fuel_purchase <- tibble()

#Where to find data for each year
for(yr in startYear:endYear) {
  writeLines("")
  writeLines(str_c("Importing ",yr,sep=""))
  
  #Determine the most recent revision
  list.files(file.path(eia923.path,"data","source",yr),pattern="^[0-9]+$") %>% 
    sort() %>% 
    last() -> year.version
  
  writeLines(str_c(" Data Version: ", year.version))
  
  year.path <- file.path(eia923.path,"data","source",yr,year.version)
  
  #print(str_c("Source Path: ", year.path))
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
  writeLines(str_c(" Filename: ", infile.name))
  filepath = file.path(year.path,infile.name)
  
  #Determine the sheet name. It starts with "Page 5"
  sheet.list <- excel_sheets(filepath)
  sheet.list.1 <- sheet.list[str_detect(str_to_upper(sheet.list),"^PAGE 5")]
  #Check that we found exactly one sheet matching this pattern
  if(length(sheet.list.1 == 1)) {
    sheet.name = sheet.list.1[1]
  } else {
    print("Cannot determine which sheet contains boiler fuel data")
    for(s in sheet.list) {
      print(s)
    }
    stop("Aborting")
  }
  writeLines(str_c(" Sheet Name: ", sheet.name))
  
  
  #Determine the row with variable names in the sheet
  #Do this by loading the first several rows of Column A
  #A look for the row containing "YEAR"
  read_excel(
    filepath,
    sheet=sheet.name,
    range="A1:A20",
    col_names="Col1"
  ) %>%
    mutate(rownum = row_number()) %>%
    filter(str_to_upper(Col1) == "YEAR") %>%
    pull(rownum) -> toprow
  
  #import data
  read_excel(filepath, sheet=sheet.name, skip=toprow-1, col_names=TRUE) -> DATA
  
  #standardize things
  DATA <- rename_all(DATA, toupper) 
  colnames(DATA)  %<>%                      #compound pipe assigns result back to colnames(DATA)
    str_replace_all( r"--{\s}--",  "") %>%
    str_replace("CONTRACTEXPIRATIONDATE", "CONTRACT_EXP_DATE") %>% #original name changes
    str_replace("AVERAGEASHCONTENT", "AVERAGE_ASH_CONTENT") %>% 
    str_replace("AVERAGEHEATCONTENT", "AVERAGE_HEAT_CONTENT") %>% 
    str_replace("AVERAGESULFURCONTENT", "AVERAGE_SULFUR_CONTENT") %>% 
    str_replace("AVERAGEMERCURYCONTENT", "AVERAGE_MERCURY_CONTENT") %>%
    str_replace("COALMINECOUNTY", "COALMINE_COUNTY") %>% 
    str_replace("COALMINEMSHAID", "COALMINE_MSHA_ID") %>% 
    str_replace("COALMINENAME", "COALMINE_NAME") %>%
    str_replace("COALMINESTATE", "COALMINE_STATE") %>% 
    str_replace("COALMINETYPE", "COALMINE_TYPE") %>% 
    str_replace("REPORTINGFREQUENCY", "RESPONDENT_FREQUENCY") %>%
    str_replace("PLANTSTATE", "STATE") %>% 
    str_replace("PURCHASETYPE", "CONTRACT_TYPE")

  #destring
  if("CONTRACT_EXP_DATE" %in% colnames(DATA)) {
    DATA %<>% mutate(CONTRACT_EXP_DATE = as.numeric(CONTRACT_EXP_DATE)) }
  if("FUEL_COST" %in% colnames(DATA)) {
    DATA %<>% mutate(FUEL_COST = as.numeric(as.character(FUEL_COST))) } #should i try to catch these warnings
  
  #drop variables(columns) containing all NAs
  NA_Cols <- character()
  for (col in colnames(DATA)) {
    if ( sum(is.na(DATA[[col]] )) == nrow(DATA) ) {
      NA_Cols <- c(NA_Cols, col ) 
  }}
  DATA <- select(DATA, -NA_Cols )  # Drop all at once
  cat("Dropped..." , length(NA_Cols), "columns b/c they contain only NA", sep=" ")
  
  #drop observations(rows) containing all NAs
  cat( "\nDropping", nrow( DATA[rowSums(is.na(DATA)) >= ncol(DATA),]), "rows because they only contain NAs", sep=" " )
  DATA <- DATA[rowSums(is.na(DATA)) < ncol(DATA),]
  
  #make sure YEAR is included
  DATA %>%
    mutate(
      YEAR = yr
    ) %>%
    #Replace "." with NA. THese are codes for missings
    mutate_if(
      is_character,
      list(~if_else(.==".",as.character(NA),.))
    ) %>%
    mutate_if(
      is_character,
      list(~if_else(.=="NA",as.character(NA),.))
    ) %>%
    #Try to convert text columns to doubles
    mutate_if(
      list(~all(!is.na(as.double(.) | is.na(.) ))), 
      as.double
    ) -> DATA
  
  
  #Append to master data set
  eia_923_fuel_purchase %<>% bind_rows(DATA)
    #if u need an import check: assign(paste("tibble",yr,sep=""), DATA)
}

#Reorder cols
col_order <- c("YEAR","MONTH","PLANTID","PLANTNAME","STATE",
               "CONTRACT_TYPE","CONTRACT_EXP_DATE","ENERGY_SOURCE","FUEL_GROUP",
               "COALMINE_TYPE","COALMINE_STATE","COALMINE_COUNTY","COALMINE_MSHA_ID","COALMINE_NAME",
               "SUPPLIER","QUANTITY","AVERAGE_HEAT_CONTENT","AVERAGE_SULFUR_CONTENT","AVERAGE_ASH_CONTENT","AVERAGE_MERCURY_CONTENT",
               "FUEL_COST","REGULATED","OPERATORNAME","OPERATORID","RESPONDENT_FREQUENCY",
               "PRIMARYTRANSPORTATIONMODE","SECONDARYTRANSPORTATIONMODE",
               "NATURALGASSUPPLYCONTRACTTYPE", "NATURALGASDELIVERYCONTRACTTYPE","NATURALGASTRANSPORTATIONSERVICE")
eia_923_fuel_purchase %<>% select(col_order, everything())


### fancy-pants missing obs. table ###

  #Doing this in 2 steps:
    #1. Find and store the missings
    #2. Make the table

#1.find and store the missings
for(yr in unique(eia_923_fuel_purchase$YEAR)) {
  YEAR_DATA <- eia_923_fuel_purchase %>% filter(YEAR == yr)   #Grab a year
  NA_Cols <- character()                                          #reset each time
  
  for (col in colnames(YEAR_DATA)) {
    if ( sum(is.na(YEAR_DATA[[col]]) ) == nrow(YEAR_DATA) ) {    #test if col is full of NAs
      NA_Cols <- c(NA_Cols, col )  }                             #Save names of NA vars  
    assign(paste("missings_",yr,sep=""), NA_Cols)                #put 'em in a "missings_year" vector
  }}

#2. Make the table
for (col in colnames(eia_923_fuel_purchase)) {
  #catch each row 
  row <- vector()
  for(yr in unique(eia_923_fuel_purchase$YEAR)) {
    #use "missings_year" vectors saved earlier 
    if (col %in% get(paste("missings_",yr,sep="")) )
    {row <-  append(row, "----" )} else {row <- append(row,yr)}
  }
  cat(row, " ")
  cat("(", col, ")", " ", sep ="")
  cat("\n")
}
# "---- indicates that the var is always missing for a given year"  #

#sort
eia_923_fuel_purchase %<>% arrange(PLANTID,YEAR,MONTH)

#OUTPUT to intermediate folder
eia_923_fuel_purchase %>%
  write_rds(file.path(eia923.path,"data","intermediate", "eia_923_fuel_purchase.rds.gz"), compress="gz")


################################
## Clean up varnames and content
################################
eia_923_fuel_purchase %>%
  mutate(
    Month = ymd(str_c(YEAR,MONTH,"01",sep="-")),
    CONTRACT_EXP_DATE = str_pad(CONTRACT_EXP_DATE,4,side="left",pad="0"),
    contract.month = str_sub(CONTRACT_EXP_DATE,1,2),
    contract.month = case_when(
      contract.month == "00" ~ "12",
      contract.month == "20" ~ "12",
      TRUE ~ contract.month
    ),
    contract.year = case_when(
      is.na(CONTRACT_EXP_DATE) ~ as.double(NA), #NA values should still be NA after conversion
      is.na(as.double(str_sub(CONTRACT_EXP_DATE,3,4))) ~ 9998, #Short circuit unparsable text
      as.double(str_sub(CONTRACT_EXP_DATE,3,4)) > 80 ~ 1900+as.double(str_sub(CONTRACT_EXP_DATE,3,4)), #Contracts in the 1900s
      as.double(str_sub(CONTRACT_EXP_DATE,3,4)) < 80 ~ 2000+as.double(str_sub(CONTRACT_EXP_DATE,3,4)), #Contracts in the 2000s
      TRUE ~ 9999 #We want to examine unhandled cases if the above logic doesn't work
    ),
    contract.expiration.date = ymd(str_c(contract.year,contract.month,"01",sep="-")),
    mmBTU.per.unit = case_when(
      #Quantity is in tons, bbl, or kcf BUT
      #AVERAGE_HEAT_CONTENT is in pounds, gallons or cf
      #On inspection this isn't right. 
      FUEL_GROUP == "Coal" ~ AVERAGE_HEAT_CONTENT,
      #Repair data entry errors in natural gas
      FUEL_GROUP == "Natural Gas" ~ if_else(AVERAGE_HEAT_CONTENT > 100, AVERAGE_HEAT_CONTENT/1000,AVERAGE_HEAT_CONTENT),
      FUEL_GROUP == "Petroleum" ~ AVERAGE_HEAT_CONTENT,
      FUEL_GROUP == "Other Gas" ~ AVERAGE_HEAT_CONTENT,
      #EIA records petroleum coke in tons The heat content around 6.287 mmBTU/bbl.
      #There are 5 bbl per short ton, or 31.435 mmBTU/ton
      FUEL_GROUP == "Petroleum Coke" ~ AVERAGE_HEAT_CONTENT
    ),
    quantity.mmBTU = QUANTITY * mmBTU.per.unit,
    #Fuel costs are in cents per physical unit
    cost.per.mmBTU = FUEL_COST / 100,
    cost.per.unit = cost.per.mmBTU * mmBTU.per.unit,
    #Is the plant regulated?
    is.regulated = case_when(
      REGULATED == "REG" ~ TRUE,
      REGULATED == "UNR" ~ FALSE,
      TRUE ~ as.logical(NA)
    ),
    annual.reporting = case_when(
      RESPONDENT_FREQUENCY == "A" ~ TRUE,
      RESPONDENT_FREQUENCY == "M" ~ FALSE,
      TRUE ~ as.logical(NA)
    )
  ) %>%
  rename(
    orispl.code = PLANTID,
    contract.type = CONTRACT_TYPE,
    energy.source = ENERGY_SOURCE,
    fuel.type = FUEL_GROUP,
    coal.mine.type = COALMINE_TYPE,
    coal.mine.state = COALMINE_STATE,
    coal.mine.count = COALMINE_COUNTY,
    coal.mine.mshaid = COALMINE_MSHA_ID,
    coal.mine.name = COALMINE_NAME,
    fuel.supplier.name = SUPPLIER,
    quantity.physical.units = QUANTITY,
    sulfur.percent = AVERAGE_SULFUR_CONTENT,
    ash.percent = AVERAGE_ASH_CONTENT,
    mercury.percent = AVERAGE_MERCURY_CONTENT,
    moisture.percent = MOISTURECONTENT,
    chlorine.percent = CHLORINECONTENT,
    transport.mode.primary = PRIMARYTRANSPORTATIONMODE,
    transport.mode.secondary = SECONDARYTRANSPORTATIONMODE,
    ng.supply.contract.type = NATURALGASSUPPLYCONTRACTTYPE,
    ng.delivery.contract.type = NATURALGASDELIVERYCONTRACTTYPE,
    ng.transport.service = NATURALGASTRANSPORTATIONSERVICE
  ) %>%
  #Remove plant details we can look up using the orispl.code
  select(-YEAR,-MONTH,-PLANTNAME,-STATE,-OPERATORNAME,-OPERATORID) %>%
  #Remove information encoded in other cleaned variables %>%
  select(
    -CONTRACT_EXP_DATE,-contract.month,-contract.year,
    -FUEL_COST,-AVERAGE_HEAT_CONTENT,
    -REGULATED,-RESPONDENT_FREQUENCY
  ) -> EIA923.clean

glimpse(EIA923.clean)

EIA923.clean %>% 
  group_by(fuel.type) %>% 
  summarize(
    min=min(cost.per.mmBTU, na.rm = TRUE),
    mean=mean(cost.per.mmBTU, na.rm = TRUE),
    median=median(cost.per.mmBTU, na.rm = TRUE),
    max= max(cost.per.mmBTU, na.rm = TRUE),
    n=n()
  )

path.out <- file.path(eia923.path,"data","out",eia923.version)
if(!dir.exists(path.out)) {
  dir.create(path.out)
  dir.create(file.path(path.out,"R"))
  dir.create(file.path(path.out,"Stata"))
}

EIA923.clean %>% 
  write_rds(file.path(path.out,"R","EIA923_fuel_purchase.rds.gz"), compress="gz") %>%
  rename_all(list(~str_replace_all(.,r"{[\s\.]}","_"))) %>%
  write_dta(file.path(path.out,"Stata","EIA923_fuel_purchase.dta"))

