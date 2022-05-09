######################################
## This program will eventually clean up the EmissionsControl tab of EIA-860's Part 6.
#####################################
library(tidyverse)
library(lubridate)
library(readxl)
library(haven)
library(assertr)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EIA-Form860/loadEnviroAssocEmissionsControlEquipment.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

#########################
## Create output data folders
#########################
path.facility.intermediate = file.path(path.project, "data","intermediate", "EPA-CEMS","facility_data")
path.facility.out = file.path(path.project, "data","out", "EPA-CEMS","facility_data")
path.facility.diagnostics = file.path(path.project,"diagnostics","EPA-CEMS","facility_data")


for(f in c(path.facility.intermediate,path.facility.out,path.facility.diagnostics)) {
  if(!dir.exists(f)) dir.create(f,recursive=TRUE)
}



g.drive = Sys.getenv("GoogleDrivePath")

eia860.path <- file.path(g.drive,"Data", "Energy","Electricity", "EIA", "Form EIA-860")

startYear <- 2018
endYear <- 2018

for(yr in startYear:endYear) {
  read_excel(
    file.path(eia860.path,"data","source",yr,str_c("6_1_EnviroAssoc_Y",yr,".xlsx")),
    sheet="Emissions Control Equipment",
    skip=1
  ) -> EmissionsEquip
}

#Define a list of equipment code to description mappings
tribble(
  ~equipment.type,~equipment.type.description,
  "JB", "Jet bubbling reactor (wet) scrubber",
  "MA","Mechanically aided type (wet) scrubber",
  "PA", "Packed type (wet) scrubber",
  "SP", "Spray type (wet) scrubber",
  "TR", "Tray type (wet) scrubber",
  "VE", "Venturi type (wet) scrubber",
  "BS", "Baghouse (fabric filter), shake and deflate",
  "BP", "Baghouse (fabric filter), pulse",
  "BR", "Baghouse (fabric filter), reverse air", 
  "EC", "Electrostatic precipitator, cold side, with flue gas conditioning",
  "EH", "Electrostatic precipitator, hot side, with flue gas conditioning",
  "EK", "Electrostatic precipitator, cold side, without flue gas conditioning", 
  "EW", "Electrostatic precipitator, hot side, without flue gas conditioning", 
  "MC", "Multiple cyclone", 
  "SC", "Single cyclone", 
  "CD", "Circulating dry scrubber", 
  "SD", "Spray dryer type / dry FGD / semi-dry FGD", 
  "DSI", "Dry sorbent (powder) injection type (DSI)",
  "ACI", "Activated carbon injection system",
  "SN", "Selective noncatalytic reduction", 
  "SR", "Selective catalytic reduction",
  "OT", "Other equipment"
) -> equipment.map

#############################
## Some hackey stuff to get SO2 equipment install dates
############################
EmissionsEquip %>%
  mutate(
    date.in.service = ymd(str_c(`Inservice Year`,`Inservice Month`,1,sep="-")),
    date.retired = ymd(str_c(`Retirement Year`,`Retirement Month`,1,sep="-")),
    acid.control = case_when(
      is.na(`Acid Gas Control?`) ~ FALSE,
      `Acid Gas Control?` == "Y" ~ TRUE,
      TRUE ~ FALSE
    )
  ) %>%
  filter(!is.na(`SO2 Control ID`)) %>%
  mutate(
    `Plant Code` = as.integer(`Plant Code`)
  ) %>%
  rename(
    orispl.code = `Plant Code`,
    equipment.type = `Equipment Type`
  ) %>%
  left_join(equipment.map,by="equipment.type") %>%
  select(orispl.code, equipment.type, equipment.type.description, date.in.service, date.retired) -> SO2.Equip


SO2.Equip %>%
  rename_all(.funs=list(~str_replace_all(.,"\\.","_"))) %>%
  write_dta(
    file.path(eia860.path,"data","intermediate","SO2_Control_Equip.dta")
  )


