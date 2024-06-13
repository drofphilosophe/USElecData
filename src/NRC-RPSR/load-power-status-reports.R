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
library(arrow)

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
na.values = c(""," ","\xc2","--",as.character(NA))

all.data = tibble()
#Loop through each year
for(yr in year.start:year.end) {
  print(sprintf("Loading %i",yr))
  filename = glue("ReactorPowerStatus_{yr}.txt.gz")
  
  #Read the TSV file. 
  read_tsv(
    file.path(path.NRC_RPSR.source,"processed-csv",filename),
    col_types = col.spec
  ) %>% 
    mutate(
      unit.name = str_to_upper(Unit),
      change.in.report = replace_na(`Change in report (*)`,"") == "*",
      date = parse_date(str_trim(Date),format="%Y-%m-%d", na=na.values),
      date.down = parse_date(str_trim(Down),"%m/%d/%Y", na=na.values),
      num.scrams = parse_number(str_trim(`Number of Scrams (#)`), na=na.values),
      power.percent = parse_integer(Power, na=na.values)
    ) %>%
    replace_na(list(num.scrams=0)) %>%
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
  distinct(unit.name) -> nomatch

if(nrow(nomatch) > 0) {
  writeLines("The following plant names from NRC data were not matched")
  print(nomatch,n=Inf)
  writeLines("You should update NRC-unit-name-to-ORIS-map.json")
  stop("Stopping")
}

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
  write_parquet(
    file.path(path.NRC.out,"NRC-ReactorPowerReports.parquet"), 
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
  write_parquet(
    file.path(path.NRC.out,"NRC-ReactorSCRAMs.parquet"), 
    compress="gz"
    )


#################################################
#################################################
## Compute hourly generation for each unit
#################################################
#################################################
#Load daily operations of each reactor from the NRC
all.data %>%
  #Add a month variable
  mutate(
    month = floor_date(date,unit="month")
  ) -> all.data

#Determine first and last dates in the data
date.start <- all.data %>% pull(month) %>% min()
date.end <- all.data %>% pull(month) %>% max()

#Load monthly generation from the EIA. Only keep nuclear reactors
read_rds(file.path(path.EIA923.out,"rds","eia_923_generation_and_fuel_wide.rds.gz")) %>% 
  filter(fuel.type == "NUC") %>%
  filter(between(year(month),year(date.start),year(date.end))) %>%
  select(orispl.code,nuclear.unit.id,month,net.elec.generation.MWh) -> eia.923





#Load a datafile of timezone info for each plant. These do not change
#over time, so just find the first one for each plant
read_rds(
  file.path(path.project,"data","out","EIA-Form860","rds","Form860_Schedule2_Plant.rds.gz")
  ) %>%
  drop_na(time.zone) %>%
  group_by(orispl.code) %>%
  summarize(time.zone = first(time.zone)) -> plant.tz

  
  

#Compute the sum of reported uptime for each reactor in each month
all.data %>%
  #Truncate date to the first of the month
  group_by(orispl.code,eia.generator.id, month) %>%
  summarize(
    power = sum(power.percent, na.rm=TRUE),
    .groups="drop"
  ) %>%
  full_join(eia.923,by=c("orispl.code", "eia.generator.id" = "nuclear.unit.id", "month")) %>%
  #Compute the daily power to hourly generation ratio
  #There are 24 hours in a day, so you need to divide by 24
  mutate(
    net.generation = if_else(net.elec.generation.MWh < 0, 0, net.elec.generation.MWh),
    power.to.generation = net.generation / power / 24
  ) -> reactor.power.to.generation

reactor.power.to.generation %>% 
  #Expand the dataset to the full date range
  group_by(orispl.code,eia.generator.id) %>%
  expand(
    month = seq.Date(date.start,date.end,by="month")
  ) %>%
  full_join(
    reactor.power.to.generation, 
    by=c("orispl.code","eia.generator.id","month")
    ) %>%
  fill(power.to.generation,.direction="downup") -> reactor.power.to.generation



all.data %>%
  #Merge the power-to-generation conversion factor into daily data
  left_join(reactor.power.to.generation, by=c("orispl.code","eia.generator.id", "month")) %>%
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
#We'll do this one plant at a time so we can easily account for time zones

#Get the list of plants
reactor.generation %>%
  group_by(orispl.code) -> reactor.generation

reactor.generation.hourly <- tibble()
for(p.data in group_split(reactor.generation)) {
  
  p = p.data$orispl.code[1]
  
  print(str_c("Plant Code:", p))
  
  #Determine the plant time zone
  plant.tz %>%
    filter(orispl.code == p) %>%
    pull(time.zone) -> tz
  
  p.data %>%
    #Count the number of hours in each day. This won't always be
    #24 hours due to daylight saving time
    mutate(
      start.hour = as_datetime(date,tz=tz),
      end.hour = as_datetime(date + days(1),tz=tz),
      num.hours = as.integer(difftime(end.hour,start.hour,units="hours"))
    ) %>%
    #expand to an hourly panel
    uncount(num.hours) %>%
    group_by(orispl.code,eia.generator.id,date) %>%
    mutate(
      datetime.utc = with_tz(start.hour,tz="UTC") + hours(row_number() - 1)
    ) %>%
    ungroup() %>%
    #Convert local time to UTC 
    mutate(
      datetime.local = force_tz(with_tz(datetime.utc, tz=tz),tz="UTC"),
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
  mutate(
    year = year(datetime.utc)
  ) %>%
  group_by(year) -> reactor.generation.hourly


for(yr.data in group_split(reactor.generation.hourly)) {
  
  yr = yr.data$year[1]
  
  writeLines(glue("Writing data for {yr}"))
  
  dir.create(file.path(path.NRC.out,"hourly"),showWarnings = FALSE,recursive = TRUE)
    
  yr.data %>%
    select(-year) %>%
    write_rds(
      file.path(path.NRC.out,"hourly",glue("Nuclear_generation_by_unit_hour_{yr}.parquet")),
      compress="gz"
    )
}







