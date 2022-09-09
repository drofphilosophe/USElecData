############################################
## netToGrossCalculation.R
##
## This program uses data in EIA 923 and CEMS to compute
## the adjustment between net generation (from EIA 923)
## and gross generation (CEMS). It also adjusts for the fact 
## that many CCGTs in CEMS only report generation from the CT
## part and ignore generation from the ST part. 
##
## Using the mapping created by CEMStoEIAMap.R, This program
## groups connected generation parts into a single unit and collects
## generation data from EIA 923's Generator Data. Some plants do not always 
## report all generation in this dataset. i.e., ORISPL 3 does not report
## the combustion turbine portion of its CCGTs prior to 2014. To account
## for this, I scale up generation at the plant-fuel-month level to match
## total generation reported in EIA 923's "Generation and Fuel" tab. This
## does not provide unit-level detail, so it is an inferior data source
#############################################
library(tidyverse)
library(lubridate)
library(haven)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EPA-CEMS/netToGrossCalculation.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

year.start <- as.integer(project.config$sources$`EIA-Form923`$`start-year`)
if(is.null(project.config$sources$`EIA-Form923`$`end-year`)) {
  year.end <- year(today())
} else {
  year.end <- as.integer(project.config$sources$`EIA-Form923`$`end-year`)
}




##################################
## CEMS to EIA crosswalk
##################################
crosswalk <- read_rds(
  file.path(path.project,"data","out","crosswalks","rds","CEMSToEIAUnitGrouping.rds.gz")
  )


###############################
###############################
## Prep EIA Data
###############################
###############################

## The generator file
read_rds(
  file.path(path.project,"data","out","EIA-Form923","rds","eia_923_generator.rds.gz")) -> eia923.generator







##You will need to true up generation in the eia923.generator since not all generating units report in that form
##But all do report in genfuel. I'll compute the ratio of generation to look for missing generation
read_rds(file.path(path.project,"data","out","EIA-Form923","rds","eia_923_generation_and_fuel_wide.rds.gz")) %>% 
  mutate(
    year = year(month),
    fuel.type = case_when(
      aer.fuel.type == "COL" ~ "COL",
      aer.fuel.type == "NG" ~ "NG",
      aer.fuel.type %in% c("PC","RFO","DFO") ~ "PET",
      TRUE ~ ""
    ),
  ) %>%
  select(month,orispl.code,fuel.type,net.elec.generation.MWh) %>%
  group_by(month,orispl.code,fuel.type) %>%
  summarize(
    totalgen.full = sum(net.elec.generation.MWh,na.rm=TRUE),
    .groups="drop"
  ) %>%
  mutate(year = year(month)) -> total.full



##Load EIA 860 to get a fuel type for each generator
read_rds(file.path(path.project,"data","out","EIA-Form860","rds","Form860_Schedule3_Generator.rds.gz")) %>% 
  select(year,orispl.code,eia.generator.id,energy.source.1) %>%
  mutate(
    fuel.type = case_when(
      energy.source.1 %in% c("BIT","SUB","LIG","WC") ~ "COL",
      energy.source.1 %in% c("NG","LFG","OBG","BFG","WH") ~ "NG",
      energy.source.1 %in% c("DFO","RFO","PC","RC","KER","JF","PET") ~ "PET",
      TRUE ~ as.character(NA)
    )
  ) %>%
  filter(!is.na(fuel.type) & fuel.type != "") %>%
  select(-energy.source.1) -> eia860.fueltype








##Crosswalk matches CEMS units to EIA generators. Since the mapping isn't 1:1
##We need to use make a 1:1 mapping on unit.group
crosswalk %>%
  distinct(orispl.code, unit.group, eia.generator.id) -> crosswalk.eia

eia860.fueltype %>%
  full_join(crosswalk.eia, by=c("orispl.code","eia.generator.id")) %>%
  replace_na(list(unit.group="0")) %>%
  mutate(year = as.integer(year)) %>%
  arrange(year,orispl.code,unit.group,fuel.type) %>%
  group_by(year,orispl.code,unit.group) %>%
  #My clever design sorts fuels the way I want to prioritize them
  #Coal then natural gas then petroleum
  summarize(
    fuel.type = first(fuel.type),
    .groups="drop"
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
  rename(month = Month) %>%
  group_by(month,orispl.code,fuel.type) %>%
  summarize(
    totgen.generator = sum(net.generation.mwh, na.rm=TRUE),
    .groups="drop"
    ) -> total.generator







######################################
## Look for plants with missing generation from their fossil fuel units
######################################
total.generator %>% 
  full_join(
    total.full,
    by=c("month","orispl.code","fuel.type")
  ) %>% 
  mutate(
    generation.factor = totalgen.full / totgen.generator,
    #Replace nonsense values of the generation factor. Anything less than one implies
    #Total generation from the plant is less than what is on the generator page, which is impossible
    #Larger than four seems unlikely. There is a large mass around 2.5, consistent with CCGTs not reporting
    #the CT part.
    generation.factor = if_else(between(generation.factor,1,4),generation.factor,1)
  ) %>% drop_na() %>%
  select(month,orispl.code,fuel.type,generation.factor) -> combined.generation

ggplot(data=filter(combined.generation,generation.factor > 1)) + 
  geom_histogram(aes(x=generation.factor),color="black",fill="blue") +
  theme_bw()
  
  
#Scale up generation in EIA923.generator
eia923.generator %>%
  left_join(
    fueltype.by.unitgroup,
    by=c("year","orispl.code","unit.group") 
  ) %>%
  left_join(
    combined.generation,
    by=c("Month"="month","orispl.code","fuel.type")
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
  replace_na(list(unit.group="")) %>%
  group_by(orispl.code,unit.group,Month) %>%
  summarize(
    eia.mwh = sum(net.generation.mwh, na.rm=TRUE),
    .groups="drop"
  ) -> eia923.generator

#Make a list of non-matching ORISPLs
eia923.generator %>%
  filter(unit.group == "") %>%
  distinct(orispl.code,unit.group,Month) -> eia923.nomatch.orispl



##########################################
## CEMS data prep
##########################################
read_rds(file.path(path.project,"data","out","EPA-CEMS","monthly","rds","CEMS_All_Months.rds.gz")) %>%
  select(orispl.code,cems.unit.id,month,elec.load.mwh,steam.load.mmbtu,total.heat.input.mmbtu,elec.heat.input.mmbtu) %>%
  rename(Month=month) %>%
  mutate(year=year(Month)) %>%
  filter(between(year,year.start,year.end)) -> cems.generator

##Prep the crosswalk for CEMS data
crosswalk %>%
  distinct(orispl.code,cems.unit.id,year,unit.group) -> crosswalk.cems

crosswalk.cems %>% 
  count(orispl.code,cems.unit.id,year) %>%
  filter(n > 1) -> duplicated.keys

if(nrow(duplicated.keys) > 0) {
  writeLines("We have duplicated keys and the following join won't work correctly. Aborting.")
  stop("Stopping")
}

#Merge in the crosswalk
cems.generator %>% 
  left_join(
    crosswalk.cems, 
    by=c("orispl.code","cems.unit.id","year")
  ) -> cems.generator

##Collapse cems units to the ORISPL level if there was no EIA match
##Otherwise collapse to the unit.group level
cems.generator %>% 
  left_join(
    eia923.nomatch.orispl,
    by=c("orispl.code","Month"),
    suffix=c("",".update")
  ) %>%
  mutate(
    unit.group = if_else(is.na(unit.group.update), unit.group, "")
  ) %>%
  group_by(orispl.code,unit.group,Month) %>%
  summarize(
    cems.mwh = sum(elec.load.mwh, na.rm = TRUE),
    .groups="drop"
  ) -> cems.generator




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
  drop_na(eia.to.cems) %>%
  arrange(orispl.code, unit.group, Month) -> combined.operations



######################################
######################################
## Clean up the ratios
######################################
######################################




#####################
## Remove unrealistic net-to-gross ratios
####################
combined.operations %>%
  rename(
    net.to.gross.ratio = eia.to.cems
  ) %>%
  #Expand the time series for each plant to the full date range to the current date
  group_by(orispl.code,unit.group) %>%
  complete(
    Month = seq.Date(ymd(str_c(year.start,"01","01",sep="-")),today(),by="month")
  ) %>%
  group_by(orispl.code,unit.group) %>%
  arrange(orispl.code,unit.group,Month) %>%
  #Compute a rolling average net-to-gross as well
  mutate(
    #Convert absurd values to NAs
    net.to.gross.ratio = if_else(between(net.to.gross.ratio,0.25,3),net.to.gross.ratio,as.double(NA)),
    #Compute some rolling averages
    ma.full = (lag(eia.mwh) + eia.mwh + lead(eia.mwh)) / (lag(cems.mwh) + cems.mwh + lead(cems.mwh)),
    ma.full = if_else(between(ma.full,0.25,3),ma.full,as.double(NA)),
    ma.doughnut = (lag(eia.mwh) + lead(eia.mwh)) / (lag(cems.mwh) + lead(cems.mwh)),
    ma.doughnut = if_else(between(ma.doughnut,0.25,3),ma.doughnut,as.double(NA))
  ) %>%
  #Convert values that differ by more than 50% of from the unit's moving average to rolling averages.
  #If that doesn't help, convert to NA
  mutate(
    deviation = abs(net.to.gross.ratio - ma.full) / ma.full,
    deviation = if_else(is.na(deviation),0,deviation),
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
    ),
    year = year(Month)
  ) %>%
  ungroup() %>%
  #Join in the cems unit IDs
  full_join(
    crosswalk.cems, by=c("orispl.code","unit.group","year")
  ) %>%
  group_by(orispl.code,cems.unit.id) %>%
  arrange(orispl.code,cems.unit.id,Month) %>%
  #Replace NAs with the lagged value or the lead value
  fill(net.to.gross.ratio,.direction="downup") %>%
  ungroup() %>%
  arrange(orispl.code,cems.unit.id,Month) -> net.2.gross.ratios



##Impute using annual and lifetime ratios
net.2.gross.ratios %>%
  group_by(orispl.code,cems.unit.id,year) %>%
  summarize(
    avg.annual.ratio = mean(net.to.gross.ratio,na.rm=TRUE),
    .groups="drop"
  ) -> annual.ratios

net.2.gross.ratios %>%
  group_by(orispl.code,cems.unit.id) %>%
  summarize(
    avg.lifetime.ratio = mean(net.to.gross.ratio,na.rm=TRUE),
    .groups="drop"
  ) -> lifetime.ratios

net.2.gross.ratios %>%
  summarize(m = median(net.to.gross.ratio,na.rm=TRUE)) %>%
  pull(m) -> overall.mean.ratio


net.2.gross.ratios %>%
  left_join(annual.ratios,by=c("orispl.code","cems.unit.id","year")) %>%
  left_join(lifetime.ratios,by=c("orispl.code","cems.unit.id")) %>%
  mutate(
    net.to.gross.ratio = case_when(
      #Keep the net-to-gross ratio if we have one
      !is.na(net.to.gross.ratio) ~ net.to.gross.ratio,
      #If not try to use an annual average
      !is.na(avg.annual.ratio) ~ avg.annual.ratio,
      #Otherwise use the lifetime average
      !is.na(avg.lifetime.ratio) ~ avg.lifetime.ratio,
      #Replace any remaining NAs with the overall sample average
      TRUE ~ overall.mean.ratio 
    )
  ) %>%
  select(orispl.code,cems.unit.id,Month,net.to.gross.ratio) %>%
  drop_na(orispl.code,cems.unit.id,Month) -> net.2.gross.ratios

#Count net-to-gross ratios for each orispl/unit
net.2.gross.ratios %>% 
  group_by(orispl.code,cems.unit.id,Month) %>% 
  summarize(n=n(),.groups="drop") %>% 
  filter(n>1) %>%
  nrow() -> row.count

if(row.count > 0) {
  writeLines("WARNING: Some Unit-Months have more than one net-to-gross ratio")
  net.2.gross.ratios %>% 
    group_by(orispl.code,cems.unit.id,Month) %>% 
    summarize(n=n(),.groups="drop") %>% 
    filter(n>1) %>%
    print(n=20)
} else {
  writeLines("Unit-Month combinations are unique")
}


#I know the output folders exist because I loaded a crosswalk from this folder as part of this script
if("rds" %in% project.local.config$output$formats) {
  net.2.gross.ratios %>%
    write_rds(
      file.path(path.project,"data","out","crosswalks","rds","EIANet_to_CEMSGross_Ratios.rds.gz"), 
      compress="gz"
    )
}

if("dta" %in% project.local.config$output$formats) {  
  net.2.gross.ratios %>%
    rename_all(.funs=list( ~ str_replace_all(.,"\\.","_"))) %>%
    write_dta(
      file.path(path.project,"data","out","crosswalks","stata","EIANet_to_CEMSGross_Ratios.dta")
    )
}
  





############################
## Make a histogram of net-to-gross ratios
############################
ggplot(data=net.2.gross.ratios) + 
  geom_histogram(
    aes(x=net.to.gross.ratio), 
    bins=50,
    color="black",
    fill="blue"
  ) +
  theme_bw() +
  labs(
    x="EIA Net to CEMS Gross Generation Ratio",
    y="count"
  ) + 
  scale_x_continuous(breaks=seq(0,3,.5)) 



dir.create(file.path(path.project,"results","diagnostics","EIANet_to_CEMSGross"), recursive = TRUE, showWarnings = FALSE)

ggsave(file.path(path.project,"results","diagnostics","EIANet_to_CEMSGross","EIANetToCEMSGrossRatio_Histogram.pdf"))
ggsave(file.path(path.project,"results","diagnostics","EIANet_to_CEMSGross","EIANetToCEMSGrossRatio_Histogram.png"))









