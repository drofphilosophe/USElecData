rm(list=ls())
library(tidyverse)
library(lubridate)
library(sf)
library(curl)
library(haven)

g.drive = Sys.getenv("GoogleDrivePath")
g.shared.drive = "G:"

cems.data <- file.path(g.shared.drive,"Shared Drives","CEMSData","data")
tz.data <- file.path(g.drive, "Data","time","time_zone_map","tz_world_mp", "World")


version.date <- "20200206" #format(now(),"%Y%m%d")

#Define the filename of the facility data file
#facility.data.filename <- "facility_03-04-2020_161549248.csv"
facility.data.filename <- "EPADownload_20220211.zip"


#Define the planar CRS for use later
#planar.crs = st_crs(2163)
planar.crs = st_crs(4326)

######################
## Load CEMS Facility data
######################
facility.coltypes = cols(
  .default = col_character(),
  `Facility ID (ORISPL)` = col_integer(),
  Year = col_integer(),
  `EPA Region` = col_integer(),
  `FIPS Code` = col_integer(),
  `Facility Latitude` = col_double(),
  `Facility Longitude` = col_double(),
  `Commercial Operation Date` = col_date(format="%m/%d/%Y"),
  `Max Hourly HI Rate (MMBtu/hr)` = col_double()
)


read_csv(
  file.path(cems.data, "source", "FacilityData", facility.data.filename),
  col_types = facility.coltypes
) %>% 
  rename(
    state.abriv = State,
    plant.name.cems = `Facility Name` ,
    orispl.code = `Facility ID (ORISPL)`,
    cems.unit.id = `Unit ID`,
    associated.stacks = `Associated Stacks`,
    year = Year,
    epa.air.programs = `Program(s)` ,
    epa.region = `EPA Region` ,
    nerc.region = `NERC Region` ,
    county.name = County ,
    county.code = `County Code` , 
    county.fips = `FIPS Code` ,
    source.category = `Source Category` ,
    plant.latitude = `Facility Latitude` ,
    plant.longitude = `Facility Longitude` ,
    plant.owner = Owner ,
    plant.operator = Operator ,
    representative.primary = `Representative (Primary)` ,
    representative.secondary = `Representative (Secondary)` ,
    SO2.phase = `SO2 Phase` ,
    NOx.phase = `NOx Phase` ,
    fuel.type.1 = `Fuel Type (Primary)` ,
    fuel.type.2 = `Fuel Type (Secondary)` ,
    SO2.controls = `SO2 Control(s)` ,
    NOx.controls = `NOx Control(s)` ,
    PM.controls = `PM Control(s)` ,
    Hg.controls = `Hg Control(s)` ,
    operation.date = `Commercial Operation Date` ,
    operating.status = `Operating Status` ,
    max.heat.input.mmbtuperhr = `Max Hourly HI Rate (MMBtu/hr)` 
  ) -> facility




###################################
###################################
## Geocode and find a local time zone for every CEMS facility
###################################
###################################

#Collapase each to a sigle year-ORISPL pair 
#Find any plants that move by more than 0.1 degrees
#Across the entire date range and add SF geometry
facility %>% 
  #Filter out Alaska, Pureto Rico, and Hawaii (they have independent grids)
  #ilter(state.abriv != "AK") %>%
  #filter(state.abriv != "HI") %>%
  group_by(orispl.code, year) %>%
  summarize(
    MinLat = min(plant.latitude),
    MinLong = min(plant.longitude),
    MaxLat = max(plant.latitude),
    MaxLong = max(plant.longitude)
  ) %>%
  mutate(
    plant.latitude = ifelse(MaxLat - MinLat < 0.01, MinLat,NA),
    plant.longitude = ifelse(MaxLong - MinLong < 0.01, MinLong, NA)
  ) %>%
  #Now group by just ORISPL
  group_by(orispl.code) %>%
  summarize(
    MinLat = min(plant.latitude),
    MinLong = min(plant.longitude),
    MaxLat = max(plant.latitude),
    MaxLong = max(plant.longitude)
  ) %>%
  mutate(
    plant.latitude = ifelse(MaxLat - MinLat < 0.01, MinLat,NA),
    plant.longitude = ifelse(MaxLong - MinLong < 0.01, MinLong, NA)
  ) %>%
  select(orispl.code, plant.latitude, plant.longitude) -> facility.geodata

#Create a dataset of only plants for which we can't establish the location
facility.geodata %>% 
  filter(is.na(plant.longitude) | is.na(plant.latitude) ) %>%
  select(orispl.code) -> facility.bad.geodata 


##Write out a CSV of facilities that we need to geocode manually
write_csv(
  facility.bad.geodata, 
  file.path(cems.data,"intermediate","facility_data","CEMS_Facilities_with_bad_geodata.csv")
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
st_read(file.path(tz.data,"tz_world_mp.shp")) %>% 
  st_transform(planar.crs) %>% 
  #Remove Antarctica
  filter( TZID != "uninhabited") %>%
  #Remove time zones that are outside the bounding box containing all CEMS plants
  filter(
    st_intersects(x=., y=facility.bbox, sparse=FALSE)
  ) -> tz.map.sf

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
ggsave(file.path(cems.data,"tz_facility_map.png"),plot=myMap.grid)

#############################
## For each facility, find the region tz.map.sf that contains it
############################
facility.geodata.sf %>%
  mutate(
    tz.id = as.integer(st_within(x=.,y=tz.map.sf, sparse=TRUE))
  ) %>%
  mutate(
    tz.name = tz.map.sf$TZID[tz.id]
  ) %>%
  mutate(
    tz.name = as.character(tz.name)
  )-> facility.geodata.with.tz

##Manual overrides
facility.geodata.with.tz %>%
  mutate(
    tz.name = if_else(state.abriv == "PR", "America/Puerto_Rico", tz.name),
    tz.id = if_else(state.abriv == "PR",0,tz.id)
  ) -> facility.geodata.with.tz





#We have some plants close to boundaries that don't return time zone information.
#We'll look them up using Google Time Zone API.
#I want to be a good citizen though. So we're going to save all 
#Time zone info I download from Google

#Create a list of plants with no time zone info
facility.geodata.with.tz %>% 
  filter(is.na(tz.name)) -> no.tz.info



#Load previous plant to time zone mappings if they exist
overrides.path = file.path(cems.data,"intermediate","facility_data","CEMS_facility_time_zone_overrides.rds")
if(file.exists(overrides.path)) {
  read_rds(overrides.path) %>%
    st_set_geometry(NULL) -> overrides
  
  #Join this information into no.tz.info
  no.tz.info %>% 
    full_join(overrides, by="orispl.code", suffix=c("",".1") ) %>%
    mutate(
      tz.name = ifelse(is.na(tz.name),tz.name.1,tz.name)
    ) %>%
    select(-ends_with(".1")) -> no.tz.info
}


#Define a function which takes a vecor of geometeries and returns a vector of 
#Time zones
getGeodata <- function(points) {
  DEBUG = FALSE
  
  result = NULL     #Storage for the result vector
  sleep.time = 5    #How many seconds to sleep between requests
  
  #LEt the user know what is happening
  n.rows = length(points)
  print(paste("Estimated time",n.rows*sleep.time,"seconds"))
  
  #Loop through the vector of geometries
  counter = 0
  for(p in points) {
    counter=counter+1
    if(DEBUG) {
      print(counter)
    }
    
    long = p[[1]]
    lat = p[[2]]
    
    #Check that we have valid geodata
    if(is.na(long) | is.na(lat)) {
      tz = NA  
    } else {
      #If we do, construct a URL and try to download info
      #Construct the API call
      url = paste0("https://maps.googleapis.com/maps/api/timezone/xml?location=", lat,",",long,"&timestamp=0")
      tryCatch({
        if(DEBUG) {
          xmlText = paste0("<time_zone_id>",intToUtf8(sample(65:90,5)),"</time_zone_id>")
        } else {
          #Be nice to the API. Sleep
          Sys.sleep(sleep.time)
          con <- curl(url,"r")
          xmlText <- paste(readLines(con), collapse='')
        }
      }, warning = function(w) {
        print(url)
        print(w)
      }, error = function(w) {
        print(url)
        print(w)
        stop("Halting execuition")
      }, finally = {
        #Close the connection regardless
        if(DEBUG == FALSE) {
          close(con)
        }
      })
    
      #You get an XML file back. Look for the <time_zone_id> tag contents
      match = str_match(xmlText,"<time_zone_id>([^<]+)</time_zone_id>")
      #We should get a 2x1 vector. We want the second value
      if(length(match) == 2) {
        tz = match[[2]]
      } else {
        tz = NA
      }
    }
    #Append the result to a vector
    result = c(result,tz)
  }
  if(DEBUG) {
    print(result)
  }
  return(result)
}


#Save all time zone overrides
no.tz.info %>%
  filter(is.na(tz.name)) %>%
  mutate(
    tz.name = getGeodata(.$geometry)
  ) %>%
  ungroup() -> google.tz.info

no.tz.info %>%
  filter(is.na(tz.name)==FALSE) %>%
  rbind(google.tz.info) -> no.tz.info

write_rds(no.tz.info,overrides.path)

no.tz.info %>%
  select(orispl.code, tz.name) %>%
  st_set_geometry(NULL) -> tz.update



#Join in the new time zone information where the old values were NA
facility.geodata.with.tz %>%
  left_join( tz.update, by="orispl.code", suffix=c("",".1") ) %>%
  mutate(
    tz.name = ifelse(is.na(tz.name), tz.name.1, tz.name)
  ) %>%
  select(-ends_with(".1")) -> facility.geodata.with.tz



#Save the results without the geometry
#We'll put geometry in the master facility data file
facility.geodata.with.tz %>%
  st_set_geometry(NULL) %>%
  write_rds(file.path(cems.data,"intermediate","facility_data","CEMS_Facility_time_zones.rds"), 
          compress="bz2"
  ) %>%
  rename_all(.funs=list(~str_replace_all(.,r"{[\.]}","_"))) %>%
  write_dta(file.path(cems.data,"intermediate","facility_data","CEMS_Facility_time_zones.dta"))



##Clean up
rm(list=c("facility.bad.geodata", 
          "facility.bbox", 
          "facility.coltypes",
          "facility.geodata",
          "facility.geodata.sf",
          "google.tz.info",
          "myMap.grid",
          "no.tz.info",
          "overrides",
          "planar.crs",
          "tz.map.sf",
          "tz.update"))

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
  distinct(epa.air.programs) -> program.list

#Count the number of commas
program.list %>%
  mutate(comma.count = str_count(epa.air.programs,",")) %>%
  summarize(n_programs = max(comma.count) + 1) %>%
  as.integer -> program.count.max

#Separate the comma separated list into a bunch of new fields
program.list %>%
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
      !!paste0("air.program.",program) := str_detect(epa.air.programs,program)
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
  left_join(facility.geodata.with.tz, by="orispl.code") -> facility

#Create a mapping between Olsen Name-Year pairs and the LST and LDT UTC offsets
facility.geodata.with.tz %>%
  select(tz.name) %>%
  st_drop_geometry() %>%
  distinct(tz.name) %>%
  na.omit() %>%
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
  
  
  
#############################
## Write ouptut file
#############################
if(!dir.exists(file.path(cems.data,"out",version.date))) {
  dir.create(file.path(cems.data,"out",version.date))
}
for(f in c( file.path(cems.data,"out",version.date,"facility_data") ) ) {
  if(!dir.exists(f)) dir.create(f)
  dir.create(file.path(f,"rds"))
  dir.create(file.path(f,"stata"))
}

facility %>%
  write_rds(file.path(cems.data,"out",version.date,"facility_data","rds","CEMS_Facility_Attributes.rds.bz2"), 
            compress="bz2")

facility %>%
  select(-geometry) %>% 
  rename_all(.funs=list( ~ str_replace_all(.,"[\\.\\s]", "_"))) %>%
  mutate_if(is_character, .funs=list(~str_sub(.,1,250))) %>%
  write_dta(file.path(cems.data,"out",version.date,"facility_data","stata","CEMS_Facility_Attributes.dta"))


  