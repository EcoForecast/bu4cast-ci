## Random Walk Null Model for Urban Thrust
# Called by run_urban_baselines.R

run_urban_random_walk <- function(reference_date, config, targets_all) {
  library(tidyverse)
  library(lubridate)
  library(tsibble)
  library(fable)
  library(aws.s3)

  reference_date <- as_date(reference_date)

  # Filter training data to <= reference_date
  targets_raw <- targets_all %>%
    filter(datetime <= as_datetime(reference_date))

  if (nrow(targets_raw) == 0) {
    message("No training data for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  # Daily (P1D)
  targets <<- targets_raw %>%
    filter(duration == "P1D") %>%
    mutate(datetime = as_date(datetime))

  site_var_daily <- expand.grid(
    site = unique(targets$site_id),
    var = unique(targets$variable),
    stringsAsFactors = FALSE
  ) %>%
    mutate(boot_number = 31,
           h = 35,
           reference_date = reference_date)

  RW_daily <- purrr::pmap_dfr(site_var_daily, RW_daily_forecast)

  RW_daily_EFI <- RW_daily %>%
    as_tibble() %>%
    rename(parameter = .rep,
           prediction = .sim) %>%
    filter(datetime > reference_date) %>%
    mutate(reference_datetime = as_datetime(reference_date),
           family = "ensemble",
           model_id = "randomWalk",
           project_id = config$project_id,
           duration = "P1D") %>%
    select(model_id, datetime, reference_datetime, site_id, family, parameter,
           variable, prediction, project_id, duration) %>%
    mutate(datetime = as_datetime(datetime),
           reference_datetime = as_datetime(reference_datetime))

  # Hourly (PT1H)
  targets <<- targets_raw %>%
    filter(duration == "PT1H") %>%
    mutate(datetime = as_datetime(datetime))

  site_var_hourly <- expand.grid(
    site = unique(targets$site_id),
    var = unique(targets$variable),
    stringsAsFactors = FALSE
  ) %>%
    mutate(boot_number = 31,
           h = 35 * 24,
           reference_date = reference_date)

  RW_hourly <- purrr::pmap_dfr(site_var_hourly, RW_hourly_forecast)

  RW_hourly_EFI <- RW_hourly %>%
    as_tibble() %>%
    rename(parameter = .rep,
           prediction = .sim) %>%
    filter(datetime >= as_datetime(reference_date + days(1))) %>%
    mutate(reference_datetime = as_datetime(reference_date),
           family = "ensemble",
           model_id = "randomWalk",
           project_id = config$project_id,
           duration = "PT1H") %>%
    select(model_id, datetime, reference_datetime, site_id, family, parameter,
           variable, prediction, project_id, duration)

  # Combine/upload

  # Compute per-variable upper caps from training data (initially predictions were sometimes negative and huge, > 2000)
  var_caps <- targets_raw %>%
    group_by(variable) %>%
    summarise(cap = 3 * quantile(observation[observation > 0], 0.99, na.rm = TRUE),
              .groups = "drop")

  RW_forecasts_EFI <- bind_rows(RW_daily_EFI, RW_hourly_EFI) %>%
    left_join(var_caps, by = "variable") %>%
    mutate(prediction = pmax(prediction, 0),
           prediction = pmin(prediction, cap)) %>%
    select(-cap)

  if (nrow(RW_forecasts_EFI) == 0) {
    message("No forecasts generated for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  forecast_file <- paste("urban", reference_date, "randomWalk.csv.gz", sep = "-")
  write_csv(RW_forecasts_EFI, forecast_file)

  aws.s3::put_object(
    file = forecast_file,
    object = paste0(config$forecasts_bucket, forecast_file),
    bucket = config$s3_bucket_write,
    base_url = gsub("https://", "", config$endpoint),
    use_https = TRUE,
    region = ""
  )

  unlink(forecast_file)
  message("Uploaded urban random walk for ", reference_date)
}
