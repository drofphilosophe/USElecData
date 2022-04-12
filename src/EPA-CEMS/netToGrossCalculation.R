############################################
## net_to_gross_adjustment.R
##
## This program uses data in EIA 923 and CEMS to compute
## the adjustment between net generation (from EIA 923)
## and gross generation (CEMS). It also adjusts for the fact 
## that many CCGTs in CEMS only report generation from the CT
## part and ignore generation from the ST part. 
##
## The bluk of the processing in this program is getting the unit matching 
## correct. Another program produces a map of CEMS to EIA units. This progra
## groups connected generation parts into a single unit and collects
## generation data from EIA 923's Generator Data. Some plants do not always 
## report all generation in this dataset. i.e., ORISPL 3 does not report
## the combustion turbine portion of its CCGTs prior to 2014. To account
## for this, I scale up generation at the plant-fuel-month level to match
## total generation reported in EIA 923's "Generation and Fuel" tab. This
## does not provide unit-level detail, so it is an inferior data source
##
##  REVISION HISTORY:
##    20200306 - File Created
##    20200511 - Program finally completed
#############################################
rm(list=ls())
library(tidyverse)
library(lubridate)
library(units)
library(haven)
library(magrittr)   #Better pipes

g.drive = Sys.getenv("GoogleDrivePath")
g.shared.drive = file.path("G:","Shared Drives")

#version.date <- format(now(),"%Y%m%d")
cems.path <- file.path(g.shared.drive,"CEMSData")
cems.version.date <- "20200206"

eia923.path <- file.path(g.drive,"Data","Energy","Electricity","EIA","Form EIA-923")
eia923.version <- "20200416"

eia860.path <- file.path(g.drive,"Data","Energy","Electricity","EIA","Form EIA-860")

#You can't start before 2008 (when EIA began providing generator data)
start.year <- 2008
end.year <- 2021

##################################
## CEMS to EIA crosswalk
##################################
crosswalk <- read_rds(
  file.path(cems.path,"data","out",cems.version.date,"to_eia","R","CEMSToEIAUnitGrouping.rds.gz")
  )







###############################
## Prep EIA Data
###############################
read_rds(file.path(eia923.path,"data","out",eia923.version,"R","EIA923_Generator.rds.gz")) %>%
  filter(between(year,start.year,end.year)) -> eia923.generator

##You will need to true up generation in the eia923.generator since not all generating units report in that form
##But all do report in genfuel. I'll compute the ratio of generation to look for missing generation
read_rds(file.path(eia923.path,"data","out",eia923.version,"R","EIA923_generation_and_fuel.rds.gz")) %>%
  mutate(
    year = year(Month),
    fuel.type = case_when(
      aer.fuel.type == "COL" ~ "COL",
      aer.fuel.type == "NG" ~ "NG",
      aer.fuel.type %in% c("PC","RFO","DFO") ~ "PET",
      TRUE ~ ""
    ),
    net.generation = drop_units(net.generation)
  ) %>%
  group_by(Month,orispl.code,fuel.type) %>%
  summarize(totalgen.full = sum(net.generation,na.rm=TRUE)) %>%
  mutate(year = year(Month)) -> total.full



##Load EIA 860 to get a fuel type for each generator
read_dta(file.path(eia860.path,"data","output","eia_860_generators.dta")) %>%
  select(Year,PlantCode,GeneratorID,EnergySource1) %>%
  #filter(between(Year,start.year,end.year)) %>%
  mutate(
    EnergySource1 = names(attributes(EnergySource1)$labels[EnergySource1]) 
  ) %>%
  mutate(
    fuel.type = case_when(
      str_detect(EnergySource1,"Coal") ~ "COL",
      str_detect(EnergySource1,"Gas") ~ "NG",
      EnergySource1 == "Waste Heat" ~ "NG",
      str_detect(EnergySource1,"Oil") ~ "PET",
      EnergySource1 %in% c("Petroelum Coke","Kerosene","Jet Fuel") ~ "PET",
      EnergySource1 %in% c("Nuclear","Solar","Wind","Geothermal","Other","Agricultural Byproducts","Black Liquor","Purchased Steam") ~ "",
      str_detect(EnergySource1, "Biomass") ~ "",
      str_detect(EnergySource1, "Waste") ~ "",
      str_detect(EnergySource1, "Water") ~ "",
      str_detect(EnergySource1, "Tire") ~ "",
      str_detect(EnergySource1, "Syngas") ~ "",
      str_detect(EnergySource1, "Energy Storage") ~ "",
      TRUE ~ as.character(NA)
    )
  ) %>%
  filter(!is.na(fuel.type) & fuel.type != "") %>%
  select(-EnergySource1) %>%
  rename(
    year = Year,
    orispl.code = PlantCode,
    eia.generator.id = GeneratorID
  ) -> eia860.fueltype








##Crosswalk matches CEMS units to EIA generators. Since the mapping isn't 1:1
##We need to use make a 1:1 mapping on unit.group
crosswalk %>%
  distinct(orispl.code, unit.group, eia.generator.id) -> crosswalk.eia

eia860.fueltype %>%
  full_join(crosswalk.eia, by=c("orispl.code","eia.generator.id")) %>%
  replace_na(list(unit.group=0L)) %>%
  mutate(year = as.integer(year)) %>%
  arrange(year,orispl.code,unit.group,fuel.type) %>%
  group_by(year,orispl.code,unit.group) %>%
  #My clever design sorts fuels the way I want to prioritize them
  #Coal then natural gas then petroleum
  summarize(
    fuel.type = first(fuel.type)
  ) -> fueltype.by.unitgroup

eia923.generator %>%
  left_join(
    crosswalk.eia,
    by=c("orispl.code","eia.generator.id")
  ) -> eia923.generator


##Compute generation by fuel in the generators set
eia923.generator %>%
  left_join(
    fueltype.by.unitgroup,
    by=c("year","orispl.code","unit.group") 
  ) %>%
  group_by(Month,orispl.code,fuel.type) %>%
  summarize(totgen.generator = sum(net.generation.mwh, na.rm=TRUE)) -> total.generator







######################################
## Look for plants with missing generation from their fossil fuel units
######################################
total.generator %>% 
  full_join(
    total.full,
    by=c("Month","orispl.code","fuel.type")
  ) %>%
  ungroup() %>%
  mutate(
    generation.factor = totalgen.full / totgen.generator,
    #Replace non-sense values of the generation factor. ANything less than one implies
    #Total generation from the plant is less than what is on the generator page, which is impossible
    #Larger than four seems unlikley. There is a large mass around 2.5, consisitent with CCGTs not reporting
    #the CT part.
    generation.factor = if_else(between(generation.factor,1,4),generation.factor,1)
  ) %>%
  select(year,Month,orispl.code,fuel.type,generation.factor) -> combined.generation

ggplot(data=combined.generation) + 
  geom_histogram(aes(x=generation.factor))
  
  
#Scale up generation in EIA923.generator
eia923.generator %>%
  left_join(
    fueltype.by.unitgroup,
    by=c("year","orispl.code","unit.group") 
  ) %>%
  left_join(
    combined.generation,
    by=c("Month","orispl.code","fuel.type")
  ) %>%
  replace_na(list(generation.factor=1)) %>%
  mutate(
    net.generation.mwh = net.generation.mwh * generation.factor
  ) %>%
  select(-fuel.type,-generation.factor) -> eia923.generator



  
##Create a list of EIA units that don't match to CEMS units
##We'll need to collapse those to the plant level

#Rebuild eia923.generator so we collapse to the unit.group level
#Plants where we can't identify the unit group should be tagged as
#having a dummy unit group 0L
eia923.generator %>%
  replace_na(list(unit.code=0L)) %>%
  group_by(orispl.code,unit.group,Month) %>%
  summarize(
    eia.mwh = sum(net.generation.mwh, na.rm=TRUE)
  ) -> eia923.generator

#Make a list of non-matching ORISPLs
eia923.generator %>%
  filter(unit.group == 0L) %>%
  distinct(orispl.code,unit.group) -> eia923.nomatch.orispl



##########################################
## CEMS data prep
##########################################
read_rds(file.path(cems.path,"data","out",cems.version.date,"monthly","rds","CEMS_by_month.rds.gz")) %>%
  select(orispl.code,cems.unit.id,month,elec.load.mwh,steam.load.mmbtu,total.heat.input.mmbtu,elec.heat.input.mmbtu) %>%
  rename(Month=month) %>%
  mutate(year=year(Month)) %>%
  filter(between(year,start.year,end.year)) -> cems.generator

##Prep the crosswalk for CEMS data
crosswalk %>%
  distinct(orispl.code,unit.group,cems.unit.id) -> crosswalk.cems

#Merge in the crosswalk
cems.generator %>% 
  left_join(
    crosswalk.cems, by=c("orispl.code","cems.unit.id")
  ) -> cems.generator

##Collapse cems units to the ORISPL level if there was no EIA match
##Otherwise collapse to the unit.group level
cems.generator %>% 
  left_join(
    eia923.nomatch.orispl,
    by="orispl.code",
    suffix=c("",".update")
  ) %>%
  mutate(
    unit.group = if_else(is.na(unit.group.update), unit.group, 0L)
  ) %>%
  group_by(orispl.code,unit.group,Month) %>%
  summarize(cems.mwh = sum(elec.load.mwh, na.rm = TRUE)) -> cems.generator




###########################################
## Combine Datasets
###########################################
eia923.generator %>%
  full_join(
    cems.generator,
    by=c("orispl.code", "unit.group", "Month")
  ) %>%
  arrange(orispl.code,unit.group,Month) %>%
  filter(eia.mwh > 0) %>%
  filter(cems.mwh > 0) %>%
  mutate(
    eia.to.cems = eia.mwh / cems.mwh
  ) %>%
  drop_na(eia.to.cems) -> combined.operations





#####################
## Save
####################
combined.operations %>%
  group_by(orispl.code,unit.group) %>%
  rename(
    net.to.gross.ratio = eia.to.cems
  ) %>%
  #Expand the time series for each plant to the full date range to the current date
  complete(
    nesting(orispl.code,unit.group),
    Month = seq.Date(ymd(str_c(start.year,"01","01",sep="-")),today(),by="month")
  ) %>%
  arrange(orispl.code,unit.group,Month) %>%
  #Compute a rolling average net-to-gross as well
  mutate(
    ma.full = (lag(eia.mwh) + eia.mwh + lead(eia.mwh)) / (lag(cems.mwh) + cems.mwh + lead(cems.mwh)),
    ma.full = if_else(between(ma.full,0.25,3),ma.full,as.double(NA)),
    ma.doughnut = (lag(eia.mwh) + lead(eia.mwh)) / (lag(cems.mwh) + lead(cems.mwh)),
    ma.doughnut = if_else(between(ma.doughnut,0.25,3),ma.doughnut,as.double(NA))
  ) %>%
  #Convert absurd values (smaller than 0.25 or larger than 3 or deviates by 50% from the moving average) to rolling averages.
  #If that doesn't help, convert to NA
  mutate(
    deviation = abs(net.to.gross.ratio - ma.full) / ma.full,
    deviation = if_else(is.na(deviation),0,deviation),
    net.to.gross.ratio = if_else(between(net.to.gross.ratio,0.25,3),net.to.gross.ratio,as.double(NA)),
    net.to.gross.ratio = case_when(
      #Everything is OK
      !is.na(net.to.gross.ratio) & between(deviation,-0.5,.5) ~ net.to.gross.ratio,
      #missing net to gross. Only the doughtnut might work
      !is.na(ma.full) ~ ma.full,
      !is.na(ma.doughnut) ~ ma.doughnut,
      #Otherwise, try using the lag
      !is.na(lag(net.to.gross.ratio)) ~ lag(net.to.gross.ratio),
      #If all else fails. Missing
      TRUE ~ as.double(NA)
    )
  ) %>%
  ungroup() %>%
  #Join in the cems unit IDs
  full_join(
    crosswalk.cems, by=c("orispl.code","unit.group")
  ) %>%
  group_by(orispl.code,cems.unit.id) %>%
  arrange(orispl.code,cems.unit.id,Month) %>%
  #Replace NAs with the lagged value
  mutate(
    net.to.gross.ratio = if_else(
      is.na(net.to.gross.ratio),
      lag(net.to.gross.ratio),
      net.to.gross.ratio
    )
  ) %>%
  ungroup() %>%
  arrange(orispl.code,cems.unit.id,Month) %>%
  mutate(
    assigned = is.na(net.to.gross.ratio)
  ) %>%
  #Replace still missings with 0.9 which is about the sample average
  replace_na(list(net.to.gross.ratio = 0.9)) %>%
  select(orispl.code,cems.unit.id,Month,net.to.gross.ratio,assigned) -> combined.operations

  




combined.operations %>%
  select(orispl.code,cems.unit.id,Month,net.to.gross.ratio) %>%
  write_rds(file.path(cems.path,"data","out",cems.version.date,"facility_data","rds","EIANet_to_CEMSGross_Ratios.rds.gz"),compress="gz") %>%
  rename_all(list(~str_replace_all(.,"[\\s\\.]","_"))) %>%
  write_dta(file.path(cems.path,"data","out",cems.version.date,"facility_data","stata","EIANet_to_CEMSGross_Ratios.dta"))
  

############################
## Make a histogram of net-to-gross ratios
############################
ggplot(data=combined.operations) + 
  geom_histogram(
    aes(x=net.to.gross.ratio,fill=assigned), 
    bins=50,
    color="black"
  ) +
  theme_bw() +
  labs(
    x="EIA Net to CEMS Gross Generation Ratio",
    y="count"
  ) + 
  scale_x_continuous(breaks=seq(0,3,.5))

if(!dir.exists(file.path(cems.path,"results",cems.version.date))) dir.create(file.path(cems.path,"results",cems.version.date))
if(!dir.exists(file.path(cems.path,"results",cems.version.date,"graphs"))) dir.create(file.path(cems.path,"results",cems.version.date,"graphs"))

ggsave(file.path(cems.path,"results",cems.version.date,"graphs","EIANetToCEMSGrossRatio_Histogram.pdf"))
ggsave(file.path(cems.path,"results",cems.version.date,"graphs","EIANetToCEMSGrossRatio_Histogram.png"))









