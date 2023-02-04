##############################
## load_ractor_power_status.R
##
## Load daily reactor power status information from the NRC
## that have been previously downloaded and parsed into 
## CSVs by getPowerReactorStatusReports.py
##############################
library(tidyverse)
library(lubridate)
library(glue)
library(here)
library(yaml)
library(jsonlite)

#Identifies the project root path using the
#relative location of this script
i_am("src/NRC-RPSR/load-power-status-reports.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

path.NRC_RPSR.source = file.path(path.project,"data","source","NRC-RPSR")
path.NRC.out = file.path(path.project,"data","out","NRC")

path.EIA923.out = file.path(path.project,"data","out","EIA-Form923")

year.start = year(ymd(project.config$sources$`NRC-RPSR`$`start-date`))
if(is.null(project.config$sources$`NRC-RPSR`$`end-date`)) {
  year.end <- year(today())
} else {
  year.end <- year(ymd(project.config$sources$`NRC-RPSR`$`end-year`))
}


#Create an ouput directory if it doesn't already exist
dir.create(path.NRC.out,recursive=TRUE,showWarnings=FALSE)

##########################
## Load a map of unit names to ORIS codes
## and EIA generator IDs
##########################
read_json(
  here(file.path("src","NRC-RPSR","NRC-unit-name-to-ORIS-map.json")),
  simplifyVector = TRUE
  ) -> unit.name.map





##Define column formats. We'll just use character as the default and parse 
##when when we read in the data
col.spec = cols(.default = col_character())
na.values = c(""," ","\xc2",NA)

all.data = tibble()
#Loop through each year
for(yr in start.year:end.year) {
  print(sprintf("Loading %i",yr))
  filename = glue("ReactorPowerStatus_{yr}.txt.gz")
  read_tsv(
    file.path(path.NRC_RPSR.source,"processed-csv",filename),
    col_types = col.spec
  ) %>%
    mutate(
      unit.name = str_to_upper(Unit),
      change.in.report = replace_na(`Change in report (*)`,"") == "*",
      date = parse_date(Date,format="%Y-%m-%d", na=na.values),
      date.down = parse_date(Down,"%m/%d/%Y", na=na.values),
      num.scrams = replace_na(parse_integer(`Number of Scrams (#)`, na=na.values),0),
      power.percent = parse_integer(Power, na=na.values)
    ) %>%
    rename(
      comment = `Reason or Comment`
    ) %>%
    select(date, unit.name, power.percent, num.scrams, date.down, change.in.report, comment) -> this.year
  
  #Append to all.data
  bind_rows(all.data,this.year) -> all.data
}

#Sort by date then unit
all.data %>%
  mutate(unit.name = str_to_upper(unit.name)) %>%
  arrange(date,unit.name) -> all.data


#Show a list of plant names that don't match
all.data %>%
  anti_join(unit.name.map, by="unit.name") %>%
  distinct(unit.name) %>%
  print(n=Inf)

all.data %>%
  left_join(unit.name.map, by="unit.name") %>%
  select(date,orispl.code,eia.generator.id,unit.name,everything()) -> all.data

#When a reactor is down, compute the date is comes back up
all.data %>%
#The reactor is down if date.down != na
  mutate(
    date.up = case_when(
      !is.na(date.down) ~ as_date(NA),
      TRUE ~ date
    )
  ) %>%
  #Fill date.up upwards
  group_by(orispl.code,eia.generator.id) %>%
  arrange(date) %>%
  fill(date.up, .direction = "up") %>%
  mutate(date.up = case_when(
    !is.na(date.down) ~ date.up,
    TRUE ~ as_date(NA)
  )
  ) %>%
  ungroup() -> all.data
  
#Write out a dataset of the power reports.)
all.data %>%
  write_rds(
    file.path(path.NRC.out,"NRC-ReactorPowerReports.rds.gz"), 
    compress="gz"
    )



################################
## Create a summary file of SCRAMs
################################
all.data %>%
  filter(num.scrams > 0) %>%
  select(date,orispl.code,eia.generator.id,date.up) -> scrams

#Write out a dataset of the power reports.)
scrams %>%
  write_rds(
    file.path(path.NRC.out,"NRC-ReactorSCRAMs.rds.gz"), 
    compress="gz"
    )


#################################################
#################################################
## Compute hourly generation for each unit
#################################################
#################################################
#Load daily operations of each reactor from the NRC
all.data
  #Add a month variable
  mutate(
    Month = floor_date(date,unit="month")
  ) -> all.data

#Determine first and last dates in the data
date.start <- reactor.report %>% pull(Month) %>% min()
date.end <- reactor.report %>% pull(Month) %>% max()

#Load monthly generation from the EIA. Only keep nuclear reactors
read_rds(file.path(path.EIA923.out,"rds","eia_923_generation_and_fuel_wide.rds.gz")) %>% 
  filter(fuel.type == "NUC") %>%
  filter(between(year(month),year(date.start),year(date.end))) %>%
  select(orispl.code,nuclear.unit.id,month,net.elec.generation.MWh) -> eia.923








#Load a datafile of timezone info for each plant
read_rds(
  file.path(path.project,"data","out","EIA-Form860","rds","Form860_Schedule2_Plant.rds.gz")
  ) %>% glimpse()
  group_by(orispl.code) %>%
  summarize(time.zone = first(time.zone)) -> plant.tz

  
#######################
## HERE
#######################
  
  

#Compute the sum of reported uptime for each reactor in each month
reactor.report %>%
  #Truncate date to the first of the month
  group_by(orispl.code,eia.generator.id, Month) %>%
  summarize(
    power = sum(power.percent, na.rm=TRUE),
    .groups="drop"
  ) %>%
  full_join(eia.923,by=c("orispl.code", "eia.generator.id" = "nuclear.unit.id", "Month")) %>%
  #Compute the daily power to hourly generation ratio
  #There are 24 hours in a day, so you need to divide by 24
  mutate(
    net.generation = units::drop_units(net.generation),
    net.generation = if_else(net.generation < 0, 0, net.generation),
    power.to.generation = net.generation / power / 24
    
  ) -> reactor.power.to.generation

reactor.power.to.generation %>% 
  #Expand the dataset to the full date range
  group_by(orispl.code,eia.generator.id) %>%
  expand(
    Month = seq.Date(date.start,date.end,by="month")
  ) %>%
  full_join(reactor.power.to.generation, by=c("orispl.code","eia.generator.id","Month")) %>%
  fill(power.to.generation,.direction="downup") -> reactor.power.to.generation

reactor.report %>%
  #Merge the power-to-generation conversion factor into daily data
  left_join(reactor.power.to.generation, by=c("orispl.code","eia.generator.id", "Month")) %>%
  #Compute hourly output
  mutate(
    net.generation = power.percent*power.to.generation
  ) %>%
  #Keep only the information we need
  select(date,orispl.code,eia.generator.id,net.generation) %>%
  #NA and -Inf imply zero net generation
  mutate(
    net.generation = case_when(
      is.na(net.generation) ~ 0,
      is.infinite(net.generation) ~ 0,
      TRUE ~ net.generation
    )
  ) -> reactor.generation


################################
## Expand to an hourly panel
################################
#We'll do this one plant at a time.
#Get the list of plants
reactor.generation %>%
  distinct(orispl.code) %>%
  pull(orispl.code) -> plant.list

reactor.generation.hourly <- tibble()
for(p in plant.list) {
  print(str_c("Plant Code:", p))
  #Determine the plant time zone
  plant.tz %>%
    filter(orispl.code == p) %>%
    pull(tz.name) -> tz
  
  reactor.generation %>%
    filter(orispl.code == p) %>%
    #Expand to an hourly panel
    mutate(
      start.hour = ymd_h(format(date ,"%Y-%m-%d 0")),
      end.hour = ymd_h(format(date + days(1),"%Y-%m-%d 0")),
      num.hours = as.integer(difftime(end.hour,start.hour,units="hours"))
    ) %>%
    #expand to an hourly panel
    uncount(num.hours) %>%
    group_by(orispl.code,eia.generator.id,date) %>%
    mutate(
      datetime.local = start.hour + hours(row_number() - 1)
    ) %>%
    ungroup() %>%
    #Convert local time to UTC 
    mutate(
      datetime.utc = with_tz(force_tz(datetime.local, tz=tz),tz="UTC"),
      utc.offset = as.integer(difftime(datetime.local,datetime.utc,units="hours"))
    ) %>%
    select(-date,-start.hour,-end.hour,-datetime.local) %>%
    rbind(reactor.generation.hourly) -> reactor.generation.hourly
}


##############################
## Write output files
## I write one per year for conveinence
##############################
reactor.generation.hourly %>%
  mutate(year = year(datetime.utc)) -> reactor.generation.hourly

reactor.generation.hourly %>%
  distinct(year) %>%
  na.omit() %>%
  pull(year) -> year.list

for(yr in year.list) {
  print(str_c("Writing ",yr))
  reactor.generation.hourly %>%
    filter(year == yr) %>%
    select(-year) %>%
    write_rds(file.path(output.dir,"rds",str_c("HourlyNuclearGenerationByGenerator_",yr,".rds.gz")), compress="gz") %>%
    rename_all(list(~str_replace_all(.,r"{[\.\s]}","_"))) %>%
    write_dta(file.path(output.dir,"stata",str_c("HourlyNuclearGenerationByGenerator_",yr,".dta")))
}







