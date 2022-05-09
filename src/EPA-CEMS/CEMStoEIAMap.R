library(tidyverse)
library(lubridate)
library(readxl)
library(matrixStats)
library(haven)
library(here)
library(yaml)

#Identifies the project root path using the
#relative location of this script
i_am("src/EPA-CEMS/CEMStoEIAMap.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

path.hourly.source = file.path(path.project, "data","source", "EPA-CEMS", "hourly")
path.facility.out = file.path(path.project, "data","out", "EPA-CEMS","facility_data")
path.cems.intermediate = file.path(path.project,"data","intermediate","EPA-CEMS")
path.facility.intermediate = file.path(path.project,"data","intermediate","EPA-CEMS","facility_data")

date.start <- ymd(str_c(project.config$sources$`EPA-CEMS`$`start-year`,"-1-1"))
if(is.null(project.config$sources$`EPA-CEMS`$`end-year`)) {
  date.end <- today()
} else {
  date.end <- ymd(str_c(project.config$sources$`EPA-CEMS`$`end-year`,"-12-31"))
}






read_excel(file.path(eia860.path,"data","source",eia.year,str_c("6_1_EnviroAssoc_Y",eia.year,".xlsx")), 
           sheet="Boiler Stack Flue", skip=1) -> EIA860.stack

read_excel(file.path(eia860.path,"data","source",eia.year,str_c("6_1_EnviroAssoc_Y",eia.year,".xlsx")), 
           sheet="Boiler Generator", skip=1) -> EIA860.boiler


read_excel(file.path(eia860.path,"data","source",eia.year,str_c("3_1_Generator_Y",eia.year,".xlsx")), 
           sheet="Operable", skip=1, guess_max = 20000) -> EIA860.generator

read_excel(file.path(eia860.path,"data","source",eia.year,str_c("3_1_Generator_Y",eia.year,".xlsx")), 
           sheet="Retired and Canceled", skip=1) %>%
  bind_rows(EIA860.generator) -> EIA860.generator

read_excel(file.path(eia860.path,"data","source",eia.year,str_c("3_1_Generator_Y",eia.year,".xlsx")), 
           sheet="Proposed", skip=1) %>%
  bind_rows(EIA860.generator) -> EIA860.generator


read_rds(file.path(cems.path,"data","out", cems.version,"facility_data","rds","CEMS_Facility_Attributes.rds.bz2")) -> CEMS.full

EIA860.generator %>%
  rename(
    orispl.code = `Plant Code`,
    eia.generator.id = `Generator ID`
  ) %>%
  distinct(orispl.code, eia.generator.id) %>%
  mutate(
    eia.unit.id = eia.generator.id,
    eia.unit.type = "generator"
  ) -> EIA.gen

EIA860.boiler %>%
  rename(
    orispl.code = `Plant Code`,
    eia.generator.id = `Generator ID`,
    eia.unit.id = `Boiler ID`
  ) %>%
  distinct(orispl.code, eia.generator.id, eia.unit.id) %>%
  mutate(
    eia.unit.type = "boiler"
  ) -> EIA.boiler


EIA860.stack %>%
  rename(
    orispl.code = `Plant Code`,
    eia.stack.id = `Stack / Flue ID`,
    eia.unit.id = `Boiler ID`
  ) %>%
  distinct(orispl.code, eia.stack.id, eia.unit.id) %>%
  left_join(EIA.boiler) %>%
  mutate(
    eia.unit.type = "stack"
  ) %>%
  select(orispl.code,eia.stack.id,eia.generator.id,eia.unit.type) %>%
  rename(
    eia.unit.id = eia.stack.id
  ) -> EIA.stack

####
# Create a single file of EIA mappings
####
EIA.gen %>%
  rbind(EIA.boiler) %>%
  rbind(EIA.stack) %>%
  arrange(orispl.code,eia.unit.id) -> EIA.full


CEMS.full %>% 
  #filter(year == cems.year) %>%
  distinct(orispl.code, cems.unit.id) %>%
  mutate(
    cems.unit.id = str_replace_all(cems.unit.id,fixed("*"),"\\*"),
    cems.unit.id = str_replace_all(cems.unit.id,fixed("#"),"\\#"),
    inCEMS = TRUE
  ) -> CEMS

##Check that CEMS unit IDs don't contain regex special chars
CEMS %>%
  filter(!str_detect(cems.unit.id,"^[A-Za-z0-9\\-\\_\\s]+$")) %>%
  distinct(cems.unit.id)

na2false <- function(v) {
  return( !is.na(v))
}

which.min.na <- function(v) {
  if(all(is.na(v))) {
    result = as.integer(NA)
  } else {
    result = which.min(v)
  }
}

row.mins.na <- function(v) {
  c = ncol(v)
  if(is.null(c) | c == 0) {
    stop("Matrix is of dimension zero. This was not expected")
  } else if(c == 1) {
    result <- v
  } else {
    result <- rowMins(v)
  }
  return(as.matrix(result))
}

adist.na <- function(list.1, list.2) {
  if(length(list.2) == 0) {
    result <- matrix(as.double(NA),nrow=rows,ncol=1)
  } else {
    result <- adist(list.1,list.2,partial=FALSE)
  }
  return(as.matrix(result))
}

#Define a function that works like adist but counts the number
#of substitutions excluding patterns that match re
adist.re <- function(list.1, list.2, re="") {
  if(re != "") {
    list.1 <- str_replace_all(list.1,re, "")
    list.2 <- str_replace_all(list.2,re, "")
  }
  return(adist.na(list.1,list.2))
}

adist.re.penalty <- function(list.1,list.2,patterns=NULL, penalties = NULL) {
  #Handle the case where list.2 is zero length
  if(length(list.2) == 0) {
    cost <- matrix(as.double(NA),nrow=rows,ncol=1)
  } else {
    #Compute the distinace with no modifications
    dist <- adist(list.1, list.2)
    cost <- dist
    #Did we provide penalties?
    if(!is.null(penalties) & !is.null(patterns)) {
      #Penalties should be numeric and patterns should be character
      if(!is.numeric(penalties)) {
        stop("penalties must be a numeric vector")
      }
      if(!is.character(patterns)) {
        stop("patterns must be a character vector")
      }
      #Penalties and patterns should be the same length
      if(length(penalties) != length(patterns)) {
        stop("penalties and patterns must be the same length")
      }
      p.len <- length(penalties)
      
      #Loop through each pattern, replace any characters in list.1
      #that matches pattern With the regex that represents pattern
      #Then compute the substitution-scored only regex adist between 
      #list.1 and list.2 This effectively IGNORES pattern
      for(p.idx in 1:p.len) {
        list.1A <- str_replace_all(list.1,patterns[p.idx],patterns[p.idx])
        #compute the modified number of subs. dist.sub should ALWAYS be
        #weakly less than dist (since the regex should always match list.1)
        dist.sub <- adist(list.1A, list.2, fixed=FALSE)
        #Apply the appropriate penalty for this new distance
        cost <- cost + penalties[p.idx]*(dist - dist.sub)
      }
    }
  }
  return(as.matrix(cost))
} 

#Test code
a <- c("CGT-1", "CGT-2", "CGT-3A")
b <- c("GT-1", "CT-2", "CGT-1A")
adist.re.penalty(a,b,patterns="[0-9]",penalties=2)

##
# CEMS %>% 
#   left_join(EIA.full, by=c("orispl.code" = "orispl.code", "cems.unit.id" = "eia.unit.id")) %>%
#   arrange(orispl.code,cems.unit.id) %>%
#   mutate(
#     matched = !is.na(eia.generator.id) 
#   ) -> JOINED


####################
## Use fuzzy matches
## Before you do this, you should make other obvious modifications
## 1. If there is only one EIA generator and one CEMS unit match them
## 2. Consider expanding things like "1-3" to observations for 1, 2, and 3
####################
#Get a list of CEMS orispl where generating units don't match
# JOINED %>%
#   filter(matched == FALSE) %>%
#   distinct(orispl.code) %>%
#   pull(orispl.code) -> nomatch.orispl

#Loop through each plant in CEMS
CEMS %>%
  distinct(orispl.code) %>%
  pull(orispl.code) %>%
  sort() -> orispl.list

all.matches <- tibble()
for(p in orispl.list) {

  print("")
  print(str_c("Processing ORISPL: ",p))
  
  #FIlter the CEMS data
  CEMS %>%
    filter(orispl.code == p) -> CEMS.filtered
  
  CEMS.filtered %>% 
    distinct(cems.unit.id) %>% 
    pull(cems.unit.id) -> cems.unit.list
  
  #Get a vector of candidate eia.unit.id from the same plant
  EIA.full %>%
    filter(orispl.code == p) %>%
    mutate(inEIA = TRUE) -> EIA.filtered
  
  EIA.filtered %>% 
    distinct(eia.unit.id) %>% 
    pull(eia.unit.id) -> eia.unit.list
  
  #####################################
  ## If there are no units to match to, flag the observations and move on
  ####################################
  if(length(eia.unit.list) == 0) {
    CEMS.filtered %>%
      mutate(
        match.method = "No EIA units"
      ) %>%
      bind_rows(all.matches) -> all.matches
    
    next
  }
  
  #########################
  ## If there is only one generating in each list use that as the match
  #########################
  if(length(cems.unit.list) == 1 & length(eia.unit.list) == 1) {
    print(str_c("ORISPL ",p," matched by Single Unit"))
    CEMS.filtered %>%
      full_join(EIA.filtered, by="orispl.code") %>%
      mutate(
        match.method = "Single Unit"
      ) %>%
      bind_rows(all.matches) -> all.matches
    
    #END THIS LOOP
    next
  }
  
  ########################
  ## If the EIA and CEMS names precisely match use that mapping
  ########################
  CEMS.filtered %>%
    full_join(EIA.filtered, 
              by=c("orispl.code" = "orispl.code", "cems.unit.id" = "eia.unit.id")
    ) -> EIA.CEMS.Joined
  
  #Look for non-matches
  EIA.CEMS.Joined %>%
    mutate(na = is.na(inCEMS) | is.na(inEIA)) %>%
    summarize(na = any(na)) %>%
    pull(na) -> na
  
  if(na == FALSE) {
    print(str_c("ORISPL ",p," matched by All Names Match"))
    EIA.CEMS.Joined %>%
      mutate(
        match.method = "All Names Match"
      ) %>%
      bind_rows(all.matches) -> all.matches
    next
  }
  
  ########################################
  ## If some subset of names match, mark those
  ## as matches and remove them from consideration
  ########################################
  CEMS.filtered %>%
    inner_join(EIA.filtered, 
              by=c("orispl.code" = "orispl.code", "cems.unit.id" = "eia.unit.id")
    ) -> EIA.CEMS.Joined
  
  if(nrow(EIA.CEMS.Joined) > 0 ) {
    print(str_c("ORISPL ",p," subset of names precisely match"))
    EIA.CEMS.Joined %>%
      mutate(
        match.method = "Subset of Names Match"
      ) %>%
      bind_rows(all.matches) -> all.matches
    
    #Remove these names from consideration
    CEMS.filtered %>%
      anti_join(EIA.CEMS.Joined, 
                by=c("orispl.code","cems.unit.id")
      ) -> CEMS.filtered
    
    #See if we are out of CEMS units to match
    if(nrow(CEMS.filtered) == 0) next
    
    EIA.filtered %>%
      anti_join(EIA.CEMS.Joined, 
                by=c("orispl.code" = "orispl.code", "eia.unit.id" = "cems.unit.id")
      ) -> EIA.filtered
    
    CEMS.filtered %>% distinct(cems.unit.id) %>% pull(cems.unit.id) -> cems.unit.list
    EIA.filtered %>% distinct(eia.unit.id) %>% pull(eia.unit.id) -> eia.unit.list
    
    #If we are out of EIA units to match, flag the remaining CEMS units and add
    #Them to the match list
    if(nrow(EIA.filtered) == 0 ) {
      CEMS.filtered %>%
        mutate(
          match.method = "No remaining EIA units"
        ) %>%
        bind_rows(all.matches) -> all.matches
      
      next
    }
    
  }
  
  ########################################
  ## Use 1:1 matches on the numeric portion of unit IDs
  ########################################
  ##################################
  ## Check to see if there is a 1:1 mapping of numeric patterns
  ## between cems.list and gen.list
  ##################################
  cems.nums <- str_extract(cems.unit.list,"([0-9]+)$")
  EIA.filtered %>% 
    distinct(eia.generator.id) %>% 
    pull(eia.generator.id) %>%
    str_extract("([0-9]+)$") %>%
    unique() -> gen.nums
  #Technically I don't look for a strictly 1:1 mapping
  #As long as every number in one list is also in the other
  if(
    all(cems.nums %in% gen.nums) & 
    all(gen.nums %in% cems.nums) & 
    any(is.na(cems.nums)) == FALSE &
    any(is.na(gen.nums)) == FALSE &
    any(cems.nums == "") == FALSE &
    any(gen.nums == "") == FALSE
  ) {
    EIA.filtered %>%
      filter(eia.unit.type == "generator") %>%
      mutate(
        unit.num = str_extract(eia.generator.id,"([0-9]+)")
      ) -> eia.unit.list
    
    CEMS.filtered %>%
      mutate(
        unit.num = str_extract(cems.unit.id,"([0-9]+)")
      ) %>% 
      full_join(eia.unit.list,by=c("orispl.code","unit.num")) %>%
      mutate(
        match.method = "Numeric Patterns"
      ) %>%
      bind_rows(all.matches) -> all.matches
    
    next
  }
  
  ################################
  ## Attempt to match units by minimizing levehinstein distance in their names
  ################################
  EIA.filtered %>%
    filter(eia.unit.type == "generator") %>%
    distinct(eia.unit.id) %>%
    pull(eia.unit.id) -> eia.gen.list
  
  EIA.filtered %>%
    filter(eia.unit.type == "boiler") %>%
    distinct(eia.unit.id) %>%
    pull(eia.unit.id) -> eia.boiler.list
  
  EIA.filtered %>%
    filter(eia.unit.type == "stack") %>%
    distinct(eia.unit.id) %>%
    pull(eia.unit.id) -> eia.stack.list
  
  rows = length(cems.unit.list)
  #Compute the distance between each item in CEMS list and each of the other lists
  gen.dist <- adist.re.penalty(cems.unit.list, eia.gen.list, patterns="[0-9]", penalties=2)
  boiler.dist <- adist.re.penalty(cems.unit.list, eia.boiler.list, patterns="[0-9]", penalties=2)
  stack.dist <- adist.re.penalty(cems.unit.list, eia.stack.list, patterns="[0-9]", penalties=2)
  
  
  #In each of these, rows are elements in cems.list and colmuns are elements in the eia list
  #Compute the rowmin for each matrix and append them
  min.dist <- cbind(row.mins.na(gen.dist), row.mins.na(boiler.dist), row.mins.na(stack.dist))
  
  #Create a vector that tells us which list has the best match
  #rows = nrow(min.dist) 
  
  
  #Create a vector that will hold an interger from 1 - 3
  #telling us which list to choose from
  list.choice <- rep(as.integer(NA),rows)
  #Create a vector that holds the index in the list we should choose from
  index.choice <- rep(as.integer(NA), rows)
  eia.unit.type.list <- rep("", rows)
  eia.unit.id.list <- rep("",rows)
  for(r in 1:rows) {
    list.choice[r] <- which.min.na(min.dist[r,])
    index.choice[r] = case_when(
      list.choice[r] == 1 ~ which.min.na(gen.dist[r,]),
      list.choice[r] == 2 ~ which.min.na(boiler.dist[r,]),
      list.choice[r] == 3 ~ which.min.na(stack.dist[r,]),
      is.na(list.choice[r]) ~ as.integer(NA),
      TRUE ~ as.integer(NA)
    )
  }
  
  eia.unit.type.list <- case_when(
    list.choice == 1 ~ "generator",
    list.choice == 2 ~ "boiler",
    list.choice == 3 ~ "stack",
    is.na(list.choice) ~ as.character(NA),
    TRUE ~ as.character(NA)
  )
  
  eia.unit.id.list <- case_when(
    list.choice == 1 ~ eia.gen.list[index.choice],
    list.choice == 2 ~ eia.boiler.list[index.choice],
    list.choice == 3 ~ eia.stack.list[index.choice],
    is.na(list.choice) ~ as.character(NA),
    TRUE ~ as.character(NA)    
  ) 
  
  #Before you merge, you need to go back to the original EIA 
  #tables and pull the corrisponding EIA generator ID for each
  #entry
  CEMS.filtered %>%
    mutate(
      eia.unit.type = eia.unit.type.list,
      eia.unit.id = eia.unit.id.list
      #Set the matched property to TRUE when we have found a match
    ) %>%
    left_join(EIA.filtered, by=c("orispl.code", "eia.unit.type", "eia.unit.id")) %>%
    mutate(
      matched = inEIA == TRUE
    ) %>%
    mutate(
      match.method = "Fuzzy Name Match"
    ) %>% 
    bind_rows(all.matches) -> all.matches
  #####END####
  next
  
  print(str_c("ORISPL ",p," FAILED TO MATCH"))
  CEMS.filtered %>%
    mutate(match.method = "Match #fail") %>%
    bind_rows(all.matches) -> all.matches
  
}

##################################
## Summarize how well we did 
##################################
all.matches %>%
  group_by(match.method) %>%
  summarize(count = n())

#View(all.matches %>% filter(match.method == "Fuzzy Name Match"))

##Remove escape characters from cems unit IDs
all.matches %>% 
  mutate(
    cems.unit.id = str_replace_all(cems.unit.id, "\\\\", "")
  ) -> all.matches

###########################################
## Create full generating unit IDs
###########################################
#We now have a mapping of cems IDs back to EIA generators
#Merge boilers back in since we need that info to match to 
#EIA-923
all.matches %>%
  select(-eia.unit.type,-eia.unit.id) %>%
  left_join(EIA.boiler,by=c("orispl.code","eia.generator.id")) %>%
  distinct(orispl.code,cems.unit.id,eia.generator.id,eia.unit.id) %>%
  rename(eia.boiler.id = eia.unit.id) %>%
  arrange(orispl.code,cems.unit.id) -> cems.2.eia

#Now you should merge in EIA-860 unit codes (which show up no later than 2015)
#And then use them to merge in unreported generators
EIA860.generator %>%
  rename(
    orispl.code = `Plant Code`,
    eia.generator.id = `Generator ID`,
    eia.unit.code = `Unit Code`
  ) %>%
  mutate(
    eia.unit.code = case_when(
      is.na(eia.unit.code) ~ eia.generator.id,
      TRUE ~ eia.unit.code
    )
  ) %>%
  distinct(orispl.code, eia.generator.id,eia.unit.code) -> EIA.units

cems.2.eia %>%
  left_join(EIA.units, by=c("orispl.code","eia.generator.id")) %>%
  left_join(EIA.units, by=c("orispl.code", "eia.unit.code"), suffix=c(".gen",".unit")) %>%
  gather(
    key="source",
    value="gen.id",
    starts_with("eia.generator.id")
  ) %>% 
  rename(eia.generator.id = gen.id) %>%
  arrange(orispl.code,cems.unit.id) %>%
  filter(!is.na(eia.generator.id)) %>%
  distinct(orispl.code, cems.unit.id, eia.boiler.id,eia.generator.id) %>%
  #Create groups that consolidate connected equipment
  #Start by grouping by CEMS unit
  group_by(orispl.code,cems.unit.id) %>%
  mutate(cems.eia.group = group_indices()) -> cems.2.eia


#Now consolidate those groups by boiler.id then generator.id
cems.2.eia %>%
  #Make a temporary boiler ID for cases where it is missing
  mutate(boiler.2 = case_when(
    is.na(eia.boiler.id) ~ cems.unit.id,
    TRUE ~ eia.boiler.id
  )
  ) -> cems.grouping

cems.grouping %>%
  group_by(orispl.code, boiler.2) %>%
  summarize(
    cems.eia.group.2 = min(cems.eia.group)
  ) %>%
  full_join(cems.grouping, by=c("orispl.code","boiler.2")) %>%
  select(-boiler.2) %>%
  #Make a temporary generator ID for cases where it is missing
  mutate(
    generator.2 = case_when(
      is.na(eia.generator.id) ~ cems.unit.id,
      TRUE ~ eia.generator.id
    )
  ) -> cems.grouping

cems.grouping %>%
  group_by(orispl.code,generator.2) %>%
  summarize(
    cems.eia.group.3 = min(cems.eia.group.2)
  ) %>%
  full_join(cems.grouping, by=c("orispl.code","generator.2")) %>%
  select(-generator.2) %>%
  arrange(orispl.code,cems.eia.group.3) %>%
  select(orispl.code,cems.eia.group.3,cems.unit.id, eia.boiler.id, eia.generator.id) %>%
  rename(unit.group = cems.eia.group.3) %>%
  ungroup() -> cems.grouping

##################################
## There will be a couple of cases a unit is assigned to multiple groups.
## Fix that
##################################
GroupsFixed = FALSE
counter = 0
while(GroupsFixed == FALSE) {
  counter=counter+1
  print(str_c("Fixing grouping for the ", counter, " time"))
  GroupsFixed = TRUE
  
  #####################
  #CEMS units
  #####################
  cems.grouping %>%
    group_by(orispl.code, cems.unit.id) %>%
    summarize(
      unit.group.update = min(unit.group), 
      max.group = max(unit.group)
    ) %>%
    filter(unit.group.update != max.group) %>%
    select(orispl.code, cems.unit.id, unit.group.update )-> group.update
  
  if(nrow(group.update) > 0) {
    print("CEMS Units")
    
    cems.grouping %>%
      left_join(
        group.update,
        by=c("orispl.code", "cems.unit.id")
      ) %>%
      mutate(
        unit.group = case_when(
          is.na(unit.group.update) ~ unit.group,
          TRUE ~ unit.group.update
        )
      ) %>%
      select(-unit.group.update) -> cems.grouping
    GroupsFixed = FALSE
  } 
 
  ##########################
  ## EIA Generators
  ##########################
  cems.grouping %>%
    group_by(orispl.code, eia.generator.id) %>%
    summarize(
      unit.group.update = min(unit.group), 
      max.group = max(unit.group)
    ) %>%
    filter(unit.group.update != max.group) %>%
    select(orispl.code, eia.generator.id, unit.group.update )-> group.update
  
  if(nrow(group.update) > 0) {
    print("EIA Generators")
    cems.grouping %>%
      left_join(
        group.update,
        by=c("orispl.code", "eia.generator.id")
      ) %>%
      mutate(
        unit.group = case_when(
          is.na(unit.group.update) ~ unit.group,
          TRUE ~ unit.group.update
        )
      ) %>%
      select(-unit.group.update) -> cems.grouping
    GroupsFixed = FALSE
  } 
  
  ##########################
  ## EIA Boilers
  ##########################
  cems.grouping %>%
    filter(!is.na(eia.boiler.id) & eia.boiler.id != "") %>%
    group_by(orispl.code, eia.boiler.id) %>%
    summarize(
      unit.group.update = min(unit.group), 
      max.group = max(unit.group)
    ) %>%
    filter(unit.group.update != max.group) %>%
    select(orispl.code, eia.boiler.id, unit.group.update )-> group.update
  
  if(nrow(group.update) > 0) {
    print("EIA Boilers")
    cems.grouping %>%
      left_join(
        group.update,
        by=c("orispl.code", "eia.boiler.id")
      ) %>%
      mutate(
        unit.group = case_when(
          is.na(unit.group.update) ~ unit.group,
          TRUE ~ unit.group.update
        )
      ) %>%
      select(-unit.group.update) -> cems.grouping
    GroupsFixed = FALSE
  } 
  
}

all.matches %>% 
  arrange(orispl.code,cems.unit.id) %>%
  View()

if(!dir.exists(file.path(cems.path,"data","out",cems.version,"to_eia"))) {
  dir.create(file.path(cems.path,"data","out",cems.version,"to_eia"))
  dir.create(file.path(cems.path,"data","out",cems.version,"to_eia","R"))
  dir.create(file.path(cems.path,"data","out",cems.version,"to_eia","Stata"))
}

all.matches %>%
  select(orispl.code,cems.unit.id,eia.generator.id,eia.unit.id,eia.unit.type,match.method) %>%
  write_rds(file.path(cems.path,"data","out",cems.version,"to_eia","R","CEMSToEIAUnitMatches.rds.gz"), compress="gz") %>%
  rename_all(.funs=list(~str_replace_all(.,"\\.","_"))) %>%
  write_dta(file.path(cems.path,"data","out",cems.version,"to_eia","Stata","CEMSToEIAUnitMatches.dta"))

cems.grouping %>%
  write_rds(file.path(cems.path,"data","out",cems.version,"to_eia","R","CEMSToEIAUnitGrouping.rds.gz"), compress="gz") %>%
  rename_all(.funs=list(~str_replace_all(.,"\\.","_"))) %>%
  write_dta(file.path(cems.path,"data","out",cems.version,"to_eia","Stata","CEMSToEIAUnitGrouping.dta"))


