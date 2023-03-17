rm(list=ls())
library(tidyverse)
library(lubridate)
library(sf)
library(curl)
library(glue)
library(haven)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EPA-CEMS/loadFacilityData.R")

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

year.start <- as.integer(project.config$sources$`EPA-CEMS`$`start-year`)
if(is.null(project.config$sources$`EPA-CEMS`$`end-year`)) {
  year.end <- year(today())
} else {
  year.end <- as.integer(project.config$sources$`EPA-CEMS`$`end-year`)
}


for(f in c(path.facility.intermediate,path.facility.out,path.facility.diagnostics)) {
  if(!dir.exists(f)) dir.create(f,recursive=TRUE)
}



#Define the planar CRS for use later
planar.crs = st_crs(4326)

######################
## Load CEMS Facility data
######################
facility.coltypes = cols(
  .default = col_character(),
  `Facility ID` = col_integer(),
  Year = col_integer(),
  `EPA Region` = col_integer(),
  `FIPS Code` = col_integer(),
  `Latitude` = col_double(),
  `Longitude` = col_double(),
  `Max Hourly HI Rate (mmBtu/hr)` = col_double()
)

facility <- tibble()
for(yr in year.start:year.end) {
  writeLines(glue("\nLoading year {yr}"))
  filepath.facility <- file.path(path.project,"data", "source", "EPA-CEMS","facility_data", glue("FacilityData_{yr}.csv.gz"))
  
  #Check to see if the source file exists
  if(!file.exists(filepath.facility)) {
    #If not, check to see if it's this year's file, which may not be published yet
    if(yr == year(today())) {
      writeLines(glue("No facility file for {yr}. This is expected before {yr} data are released, typically in April"))
      writeLines("Skipping")
      #Move on to the next loop iteration (which should end the loop)
      next
    } else {
      #If it's not the current year file that is missing, exit with a non-zero status
      writeLines(glue("No facility data for {yr}. Resource CEMS data before proceeding."))
      writeLines("Exiting")
      quit(save="no",status=-1)
    }
  }
  
  read_csv(
    filepath.facility,
    col_types = facility.coltypes
  ) %>% 
    mutate(`Commercial Operation Date` = ymd(`Commercial Operation Date`)) %>%
    rename(
      state.abriv = State,
      plant.name.cems = `Facility Name` ,
      orispl.code = `Facility ID`,
      cems.unit.id = `Unit ID`,
      associated.stacks = `Associated Stacks`,
      year = Year,
      epa.air.programs = `Program Code` ,
      epa.region = `EPA Region` ,
      nerc.region = `NERC Region` ,
      county.name = County ,
      county.code = `County Code` , 
      county.fips = `FIPS Code` ,
      source.category = `Source Category` ,
      plant.latitude = `Latitude` ,
      plant.longitude = `Longitude` ,
      plant.owner = `Owner/Operator` ,
      SO2.phase = `SO2 Phase` ,
      NOx.phase = `NOx Phase` ,
      fuel.type.1 = `Primary Fuel Type` ,
      fuel.type.2 = `Secondary Fuel Type` ,
      SO2.controls = `SO2 Controls` ,
      NOx.controls = `NOx Controls` ,
      PM.controls = `PM Controls` ,
      Hg.controls = `Hg Controls` ,
      operation.date = `Commercial Operation Date` ,
      operating.status = `Operating Status` ,
      max.heat.input.mmbtuperhr = `Max Hourly HI Rate (mmBtu/hr)`,
      associated.generators.capacity = `Associated Generators & Nameplate Capacity (MWe)`
    ) %>%
    bind_rows(facility) -> facility
}


#########################################
## Load Legacy Facility Data
## This is facility info that is no longer reported in CAMPD
#########################################
read_csv(
  file.path(here("src","EPA-CEMS","legacyFacilityData.csv.gz")),
  col_types = facility.coltypes
) %>% 
  mutate(`Commercial Operation Date` = mdy(`Commercial Operation Date`)) %>%
  rename(
    state.abriv = State,
    plant.name.cems = `Facility Name` ,
    orispl.code = `Facility ID`,
    cems.unit.id = `Unit ID`,
    year = Year,
    associated.stacks = `Associated Stacks`,
    epa.air.programs = `Program Code` ,
    epa.region = `EPA Region` ,
    nerc.region = `NERC Region` ,
    county.name = County ,
    county.code = `County Code` , 
    county.fips = `FIPS Code` ,
    source.category = `Source Category` ,
    plant.latitude = `Latitude` ,
    plant.longitude = `Longitude` ,
    plant.owner = `Owner/Operator` ,
    SO2.phase = `SO2 Phase` ,
    NOx.phase = `NOx Phase` ,
    fuel.type.1 = `Primary Fuel Type` ,
    fuel.type.2 = `Secondary Fuel Type` ,
    SO2.controls = `SO2 Controls` ,
    NOx.controls = `NOx Controls` ,
    PM.controls = `PM Controls` ,
    Hg.controls = `Hg Controls` ,
    operation.date = `Commercial Operation Date` ,
    operating.status = `Operating Status` ,
    max.heat.input.mmbtuperhr = `Max Hourly HI Rate (mmBtu/hr)`,
    associated.generators.capacity = `Associated Generators & Nameplate Capacity (MWe)`
  ) -> legacy.facility

#Detect legacy facility entries that happen to be in our data
#Remove them and bind the remainder to the facility data
legacy.facility %>%
  anti_join(facility,by=c("orispl.code","cems.unit.id","year")) %>%
  bind_rows(facility) -> facility




###################################
###################################
## Geocode and find a local time zone for every CEMS facility
###################################
###################################

#Collapase each to a single year-ORISPL pair 
#Find any plants that move by more than 0.1 degrees
#Across the entire date range and add SF geometry
facility %>% 
  #Filter out Alaska, Pureto Rico, and Hawaii (they have independent grids)
  #ilter(state.abriv != "AK") %>%
  #filter(state.abriv != "HI") %>%
  group_by(orispl.code, year) %>%
  summarize(
    MinLat = min(plant.latitude,na.rm=TRUE),
    MinLong = min(plant.longitude,na.rm=TRUE),
    MaxLat = max(plant.latitude,na.rm=TRUE),
    MaxLong = max(plant.longitude,na.rm=TRUE),
    state.abriv = first(state.abriv),
    .groups="drop"
  ) %>%
  mutate(
    plant.latitude = ifelse(MaxLat - MinLat < 0.01, MinLat,NA),
    plant.longitude = ifelse(MaxLong - MinLong < 0.01, MinLong, NA)
  ) %>%
  #Now group by just ORISPL
  group_by(orispl.code) %>%
  summarize(
    MinLat = min(plant.latitude,na.rm=TRUE),
    MinLong = min(plant.longitude,na.rm=TRUE),
    MaxLat = max(plant.latitude,na.rm=TRUE),
    MaxLong = max(plant.longitude,na.rm=TRUE),
    state.abriv = first(state.abriv)
  ) %>%
  mutate(
    plant.latitude = ifelse(MaxLat - MinLat < 0.01, MinLat,NA),
    plant.longitude = ifelse(MaxLong - MinLong < 0.01, MinLong, NA)
  ) %>%
  select(orispl.code, plant.latitude, plant.longitude,state.abriv) -> facility.geodata

#Create a dataset of only plants for which we can't establish the location
facility.geodata %>% 
  drop_na(plant.latitude,plant.longitude) %>%
  select(orispl.code) -> facility.bad.geodata 


##Write out a CSV of facilities that we need to geocode manually
write_csv(
  facility.bad.geodata, 
  file.path(path.facility.intermediate,"CEMS_Facilities_with_bad_geodata.csv")
)

##Here you should load in updates for the bad geometries and merge them back into 
##the facility data before creating the geometry objects.
facility.geodata %>%
  mutate(
    plant.latitude = case_when(
      orispl.code == 50358 ~ 42.149465,
      orispl.code == 54620 ~ 42.5428,
      orispl.code == 55704 ~ 47.130032,
      orispl.code == 60345 ~ 27.634167,
      TRUE ~ plant.latitude),
    
    plant.longitude = case_when(
      orispl.code == 50358 ~ -80.030875,
      orispl.code == 54620 ~ -71.8528,
      orispl.code == 55704 ~ -122.37945,
      orispl.code == 60345 ~ -80.791111,
      TRUE ~ plant.longitude)   
  ) -> facility.geodata

##Add sf data to the tibble
facility.geodata %>%
  st_as_sf(
    coords=c("plant.longitude","plant.latitude"), 
    na.fail=FALSE,
    crs=planar.crs
  ) %>%
  st_set_crs(planar.crs) -> facility.geodata.sf


##Create a bounding box around all facilities
facility.geodata.sf %>%
  st_bbox(crs=planar.crs) %>%
  st_as_sfc(crs=planar.crs) -> facility.bbox

st_crs(facility.bbox) = planar.crs

#######################
## Load time zone shapefile, transform to planar projection
## And limit to features that intersect the facility bounding box
#######################
sf::sf_use_s2(FALSE)
st_read(
  file.path(path.project,"data","source","tz-info","tz-geodata-combined-with-oceans.json"),
  drivers = "GeoJSON"
  )  %>% 
  st_transform(planar.crs) %>%
  rename(TZID = tzid) %>%
  #Remove Antarctica
  filter( TZID != "uninhabited") %>%
  #Remove time zones that are outside the bounding box containing all CEMS plants
  st_filter(facility.bbox, .predicate=st_intersects) -> tz.map.sf

#Show us what you got  
print("Facility BBOX:")
print(st_bbox(facility.bbox))
print("TZ BBOX:")
print(st_bbox(tz.map.sf))

#Print out a map of the regions we're going to try and match to CEMS facilities
myMap.grid = ggplot() + #Open a plot
  geom_sf(data = facility.bbox, color='red', fill='blue') +  #Plot the shapefile
  geom_sf(data = tz.map.sf, fill='white') +  #Plot the shapefile
  geom_sf(data = facility.geodata.sf, color='black', size=.5, shape=".")

#print(myMap.grid)
ggsave(file.path(path.facility.diagnostics,"tz_facility_map.png"),plot=myMap.grid)

#############################
## For each facility, find the region tz.map.sf that contains it
############################
facility.geodata.sf %>%
  st_join(tz.map.sf) %>%
  rename(tz.name = TZID) -> facility.geodata.with.tz


##Manual overrides
facility.geodata.with.tz %>%
  mutate(
    tz.name = if_else(state.abriv == "PR", "America/Puerto_Rico", tz.name)
  ) -> facility.geodata.with.tz


writeLines("Plants with no time zone information")
facility.geodata.with.tz %>% 
  filter(is.na(tz.name))


##Update time zone information manually
facility.geodata.with.tz %>%
  mutate(tz.name = case_when(
    orispl.code %in% c(3199, 10331, 10614, 10662, 10747, 50460, 880073) ~ "America/New_York",
    TRUE ~ tz.name
  )) -> facility.geodate.with.tz








#Save the results without the geometry
#We'll put geometry in the master facility data file
if("rds" %in% project.local.config$output$formats) {
  facility.geodata.with.tz %>%
    st_drop_geometry() %>%
    write_rds(file.path(path.facility.intermediate,"CEMS_Facility_time_zones.rds"), 
              compress="bz2"
    )
}

if("dta" %in% project.local.config$output$formats) {
  facility.geodata.with.tz %>%
    st_drop_geometry() %>%
    rename_all(.funs=list(~str_replace_all(.,r"{[\.]}","_"))) %>%
    write_dta(file.path(path.facility.intermediate,"CEMS_Facility_time_zones.dta"))
}
  



##Clean up
rm(list=c("facility.bad.geodata", 
          "facility.bbox", 
          "facility.coltypes",
          "facility.geodata",
          "facility.geodata.sf",
          "myMap.grid",
          "planar.crs",
          "tz.map.sf"))

###############################################
###############################################
## Further Facility Data cleaning
###############################################
###############################################

##################################################
#convert epa.air.programs to a vectors of logicals
#specifying the programs governing the plant
##################################################
facility %>%
  distinct(epa.air.programs) %>%
  drop_na() -> program.list

#Count the number of commas
program.list %>%
  mutate(comma.count = str_count(epa.air.programs,",")) %>%
  summarize(n_programs = max(comma.count) + 1) %>%
  as.integer -> program.count.max

#Separate the comma separated list into a bunch of new fields
program.list %>%
  drop_na() %>%
  separate(
    epa.air.programs, 
    into=str_c("X.",1:program.count.max),
    sep=",",
    fill="right") %>%
  #Convert to a long list of programs
  gather(starts_with("X."),key="Key", value=program.name) %>%
  #Remove excess spaces
  mutate(program.name = str_trim(program.name)) %>%
  #Remove empty and NA values
  filter(program.name != "") %>%
  filter(!is.na(program.name)) %>%
  #Make a distinct list
  distinct(program.name) %>%
  #Save as a vector
  pull(program.name) -> program.list

#loop through each program and create a variable set to TRUE
#If that plant is covered by that program adn FALSE otherwise
facility.copy <- facility
for(program in program.list) {
  facility.copy %>%
    #Starting in version 0.6 dplyr allows indirect naming of variables
    mutate(
      !!str_c("air.program.",program) := str_detect(epa.air.programs,program)
    ) -> facility.copy
}
facility.copy %>%
  select(-epa.air.programs) -> facility

rm(list=c("facility.copy", "program.list","program.count.max"))


###############################
## Remove some redundant geography fields
###############################
facility %>%
  select(-county.name,-county.code) -> facility


################################
## Clean up the "operating" field
################################
#What kind of dates do we have?
facility %>%
  select(operating.status) %>%
  mutate(
    date.type = str_match(operating.status,"\\(([^)]+) [012]?[0-9]/[0123]?[0-9]/[12][90][0-9][0-9]\\)")[,2]
  ) %>%
  distinct(date.type) %>%
  pull(date.type) -> date.types
print(date.types)


facility %>%
  mutate(
    plant.operational = str_detect(operating.status,"Operating"),
    plant.retired = str_detect(operating.status,"Retired"),
    plant.future = str_detect(operating.status,"Future"),
    plant.cold.storage = str_detect(operating.status,"Cold Storage"),
    plant.started.text = str_match(operating.status,"Started ([012]?[0-9]/[0123]?[0-9]/[12][90][0-9][0-9])")[,2],
    plant.started.date = as_date(strptime(plant.started.text, "%m/%d/%Y")),
    plant.retired.text = str_match(operating.status,"Retired ([012]?[0-9]/[0123]?[0-9]/[12][90][0-9][0-9])")[,2],
    plant.retired.date = as_date(strptime(plant.retired.text, format="%m/%d/%Y"))
  ) %>%
  select(-operating.status, -plant.started.text, -plant.retired.text) -> facility



###############################
## Add geocoding in
###############################

#Merge the geocodes and time zones back into the facility data file
facility %>%
  select(-plant.latitude,-plant.longitude) %>%
  left_join(facility.geodata.with.tz, by="orispl.code", suffix=c("",".yyyy")) %>%
  select(-ends_with(".yyyy")) -> facility

#Create a mapping between Olsen Name-Year pairs and the LST and LDT UTC offsets
facility.geodata.with.tz %>%
  select(tz.name) %>%
  st_drop_geometry() %>%
  distinct(tz.name) %>%
  drop_na() %>%
  pull(tz.name) -> tz.name.list

facility %>%
  distinct(year) %>%
  pull(year) -> year.list

tz.offset.by.year <- tibble()
for(t in tz.name.list) {
  #Compute the LST and DST offsets in each year.
  for(y in year.list) {
    #I'm going to make the bold assumption that 1-1-YEAR is LST and 7-1-YEAR is DST
    time.st <- ymd_h(str_c(y,"-01-01 00"))
    time.dt <- ymd_h(str_c(y,"-07-01 00"))
    tz.offset.st <- as.integer( difftime( force_tz(time.st,tz="UTC"), force_tz(time.st,tz=t),units="hours") ) 
    tz.offset.dt <- as.integer( difftime( force_tz(time.dt,tz="UTC"), force_tz(time.dt,tz=t),units="hours") )
    
    tz.offset.by.year %>%
      rbind(
        tibble(
          tz.name=t,
          year=y,
          utc.offset.st=tz.offset.st,
          utc.offset.dt=tz.offset.dt
          )
        ) -> tz.offset.by.year
  }
}

######################
## Categorize fuel types into
## Coal
## Natural Gas
## Petroleum
## Other Solid fuel
######################
facility %>%
  mutate(
    fuel.type.coal = str_detect(fuel.type.1,"Coal"),
    fuel.type.natural.gas = str_detect(fuel.type.1,"Gas"),
    fuel.type.petroleum = str_detect(fuel.type.1, "Oil"),
    fuel.type.other.solid = str_detect(fuel.type.1, "Wood") | 
      str_detect(fuel.type.1, "Tire") | str_detect(fuel.type.1,"Other Solid") | 
      str_detect(fuel.type.1,"Coke"),
    fuel.type.natural.gas = fuel.type.natural.gas & !(fuel.type.coal | fuel.type.petroleum | fuel.type.other.solid),
    fuel.type.petroleum = fuel.type.petroleum & !(fuel.type.coal | fuel.type.other.solid),
    fuel.type.other.solid = fuel.type.other.solid & !fuel.type.coal
  ) %>%
  #Merge in timezone offsets
  left_join(tz.offset.by.year,by=c("tz.name","year")) -> facility

facility %>%
  mutate(
    plant.latitude = st_coordinates(geometry)[,2],
    plant.longitude = st_coordinates(geometry)[,1]
  ) %>%
  st_drop_geometry() -> facility

  
  
#############################
## Write ouptut file
#############################
if("rds" %in% project.local.config$output$formats) {
  dir.create(file.path(path.facility.out,"rds"),showWarnings = FALSE)

  facility %>%
    write_rds(file.path(path.facility.out,"rds","CEMS_Facility_Attributes.rds.bz2"), 
              compress="bz2")
}

if("dta" %in% project.local.config$output$formats) {
  dir.create(file.path(path.facility.out,"stata"),showWarnings = FALSE)

  
  facility %>%
    select(-geometry) %>% 
    rename_all(.funs=list( ~ str_replace_all(.,"[\\.\\s]", "_"))) %>%
    mutate_if(is_character, .funs=list(~str_sub(.,1,250))) %>%
    write_dta(file.path(path.facility.out,"stata","CEMS_Facility_Attributes.dta"))
}



  