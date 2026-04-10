## Coastal Targets Correction Script for Coastal Thrust
## Reads uncorrected coastal-targets.csv, applies ratio-based corrections, and writes to S3
## Author: Cami Webb, cwebb16@bu.edu

library(arrow)
library(dplyr)
library(lubridate)
library(tidyr)
library(yaml)
library(RCurl)

config <- yaml::read_yaml("challenge_configuration.yaml")

project_id     <- config$project_id
raw_filename   <- config$target_groups$Coastal$targets_filepath
corr_filename  <- config$target_groups$Coastal$targets_corrected_filepath

s3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

message("Reading uncorrected targets from S3...")
raw_data <- arrow::read_csv_arrow(s3$path(raw_filename)) %>% as.data.frame()
message("Rows in raw data: ", nrow(raw_data))

## Compute correction factors (Option A: cutoff first, then monthly median)
message("Computing correction factors...")

paired_hist <- raw_data %>%
  dplyr::filter(variable %in% c("chlora_buoy", "chlora_cci")) %>%
  dplyr::mutate(date = as.Date(substr(datetime, 1, 10))) %>%
  dplyr::select(variable, date, observation) %>%
  dplyr::filter(!is.na(observation), observation > 0) %>%
  tidyr::pivot_wider(
    names_from  = variable,
    values_from = observation,
    values_fn   = list(observation = mean)
  ) %>%
  dplyr::filter(!is.na(chlora_buoy), !is.na(chlora_cci), chlora_buoy > 0.001) %>%
  dplyr::mutate(
    ratio = chlora_cci / chlora_buoy,
    month = lubridate::month(date)
  ) %>%
  dplyr::filter(ratio <= 5 & ratio >= 1/5)

exclude_dates <- raw_data %>%
  dplyr::filter(variable %in% c("chlora_buoy", "chlora_cci")) %>%
  dplyr::mutate(date = as.Date(substr(datetime, 1, 10))) %>%
  dplyr::select(variable, date, observation) %>%
  dplyr::filter(!is.na(observation), observation > 0) %>%
  tidyr::pivot_wider(
    names_from  = variable,
    values_from = observation,
    values_fn   = list(observation = mean)
  ) %>%
  dplyr::filter(!is.na(chlora_buoy), !is.na(chlora_cci)) %>%
  dplyr::mutate(ratio = chlora_cci / chlora_buoy) %>%
  dplyr::filter(ratio > 5 | ratio < 1/5 | chlora_buoy <= 0.001) %>%
  dplyr::pull(date) %>%
  unique()

message("Dates excluded by ratio cutoff: ", length(exclude_dates))

correction_factor <- paired_hist %>%
  dplyr::group_by(month) %>%
  dplyr::summarise(median_ratio = median(ratio, na.rm = TRUE), .groups = "drop")

message("Monthly correction factors:")
print(correction_factor)

## Apply corrections
message("Applying corrections to CCI data...")

buoy_data <- raw_data %>%
  dplyr::filter(variable == "chlora_buoy")

cci_corrected <- raw_data %>%
  dplyr::filter(variable == "chlora_cci") %>%
  dplyr::mutate(date = as.Date(substr(datetime, 1, 10))) %>%
  dplyr::filter(!date %in% exclude_dates) %>%
  dplyr::mutate(month = lubridate::month(date)) %>%
  dplyr::left_join(correction_factor, by = "month") %>%
  dplyr::mutate(
    observation = dplyr::if_else(
      !is.na(median_ratio) & median_ratio > 0,
      observation / median_ratio,
      observation
    )
  ) %>%
  dplyr::select(-month, -median_ratio, -date) %>%
  dplyr::mutate(variable = "chlora_cci_corrected")

corrected_data <- dplyr::bind_rows(buoy_data, cci_corrected) %>%
  dplyr::arrange(site_id, datetime, variable)

message("Rows in corrected data: ", nrow(corrected_data))

## Write to S3
message("Writing corrected targets to S3...")
arrow::write_csv_arrow(corrected_data, sink = s3$path(corr_filename))

message("Pinging health check...")
tryCatch(
  RCurl::getURL(config$target_groups$Coastal$health_check_url_c),
  error = function(e) message("Health check ping failed: ", e$message)
)

message("Coastal targets correction script complete!")
