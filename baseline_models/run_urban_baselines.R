library(tidyverse)
library(lubridate)
library(aws.s3)
library(yaml)
library(httr)

source("baseline_models/R/randomWalkDailyFunction.R")
source("baseline_models/R/randomWalkHourlyFunction.R")
source("baseline_models/models/urban_climatology.R")
source("baseline_models/models/urban_random_walk.R")

Sys.setenv("AWS_DEFAULT_REGION" = "")

config <- yaml::read_yaml("challenge_configuration.yaml")
null_start_date <- as_date(config$target_groups$Urban$null_start_date)
base_url <- gsub("https://", "", config$endpoint)

# Read target
targets_url <- paste0(config$endpoint, "/", config$s3_bucket_read, "/",
                      config$target_groups$Urban$targets_filepath)
targets_all <- readr::read_csv(targets_url, guess_max = 10000) %>%
  mutate(datetime = as_datetime(datetime))

# Read site metadata 
metadata_url <- paste0(config$endpoint, "/", config$s3_bucket_read, "/",
                       config$target_groups$Urban$site_metadata_filepath)
sites_metadata <- readr::read_csv(metadata_url)

# Reference dates = null_start_date to yesterday
all_dates <- seq(null_start_date, Sys.Date() - 1, by = "day")

# Get reference dates from file names
get_existing_dates <- function(model_name) {
  get_dates_from_prefix <- function(prefix) {
    tryCatch({
      files <- aws.s3::get_bucket_df(
        bucket = config$s3_bucket_write,
        prefix = paste0(prefix, "/urban-"),
        base_url = base_url,
        use_https = TRUE,
        region = "",
        max = Inf
      )
      if (nrow(files) == 0) return(as_date(character(0)))
      files %>%
        pull(Key) %>%
        .[stringr::str_detect(., model_name)] %>%
        stringr::str_extract("\\d{4}-\\d{2}-\\d{2}") %>%
        as_date() %>%
        na.omit()
    }, error = function(e) as_date(character(0)))
  }
  unique(c(
    get_dates_from_prefix(config$forecasts_bucket),
    get_dates_from_prefix("challenges/project_id=bu4cast/raw-submissions")
  ))
}

# Climatology backfill
existing <- get_existing_dates("climatology")
missing  <- all_dates[!all_dates %in% existing]
message(length(missing), " urban climatology dates to run")
for (ref_date in as.list(missing)) {
  run_urban_climatology(as_date(ref_date), config, targets_all, sites_metadata)
}
httr::GET(config$target_groups$Urban$health_checks$climatology_null) # health check

# Random walk backfill
existing <- get_existing_dates("randomWalk")
missing  <- all_dates[!all_dates %in% existing]
message(length(missing), " urban random walk dates to run")
for (ref_date in as.list(missing)) {
  run_urban_random_walk(as_date(ref_date), config, targets_all)
}
httr::GET(config$target_groups$Urban$health_checks$random_walk_null) # health check
