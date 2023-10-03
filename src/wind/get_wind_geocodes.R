################################################
## Load boiler-stack associations from EIA Form 860 Schedule 6
################################################
library(tidyverse)
library(lubridate)
library(here)
library(yaml)
library(glue)
library(arrow)

#Identifies the project root path using the
#relative location of this script
i_am("src/EIA-Form860/loadEIA860_Schedule2_Plants.R")

#Read the project configuration
read_yaml(here("config.yaml")) -> project.config
read_yaml(here("config_local.yaml")) -> project.local.config

path.project <- file.path(project.local.config$output$path)
version.date <- project.config$`version-info`$`version-date`

year.start = as.integer(project.config$sources$`EIA-Form860`$`start-year`)
if(is.null(project.config$sources$`EIA-Form860`$`end-year`)) {
  year.end <- year(today())
} else {
  year.end <- as.integer(project.config$sources$`EIA-Form860`$`end-year`)
}

read_rds(
  file.path(path.project,"data","out","EIA-Form860","rds","Form860_Schedule3_Generator.rds.gz")
) |> 
  filter(prime.mover %in% c("WS","WT")) |>
  distinct(orispl.code) |>
  drop_na() |>
  arrange(orispl.code) -> wind.generators

read_rds(
  file.path(path.project,"data","out","EIA-Form860","rds","Form860_Schedule2_Plant.rds.gz")
) |>
  dplyr::select(orispl.code, plant.latitude, plant.longitude) |>
  #Restrict data to a bounding box around the coterminous US
  filter(between(plant.latitude,24,50)) |>
  filter(between(plant.longitude,-126,-66)) |>
  drop_na() |>
  right_join(wind.generators,join_by(orispl.code)) |>
  group_by(orispl.code,plant.latitude,plant.longitude) |>
  summarize(count=n(),.groups="drop") |>
  group_by(orispl.code) |>
  arrange(orispl.code,desc(count)) |>
  summarize(across(everything(),~first(.))) |>
  dplyr::select(-count) |>
  drop_na() -> plants

dir.create(file.path(path.project,"data","intermediate","wind"),recursive = TRUE, showWarnings = FALSE)
plants |>
  write_parquet(
    file.path(path.project,"data","intermediate","wind","wind_turbine_geocodes.parquet")
  )


