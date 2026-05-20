## Random Walk Null Model for Coastal Thrust
# Called by run_coastal_baselines.R

run_coastal_random_walk <- function(reference_date, config, targets_all) {
  library(tidyverse)
  library(lubridate)
  library(tsibble)
  library(fable)
  library(aws.s3)

  reference_date <- as_date(reference_date)

  # Filter training data to <= reference_date
  targets <<- targets_all %>%
    filter(datetime <= reference_date,
           variable == "chlora_cci_corrected")

  if (nrow(targets) == 0 || all(is.na(targets$observation))) {
    message("No training data for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  horizon <- config$target_groups$Coastal$group_vars$chlora_cci$max_horizon

  site_var_combinations <- expand.grid(
    site = unique(targets$site_id),
    var  = unique(targets$variable),
    stringsAsFactors = FALSE
  ) %>%
    mutate(boot_number = 31,
           h = horizon,
           reference_date = reference_date)

  RW_forecasts <- purrr::pmap_dfr(site_var_combinations, RW_daily_forecast)

  if (nrow(RW_forecasts) == 0) {
    message("No forecasts generated for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  RW_forecasts_EFI <- RW_forecasts %>%
    as_tibble() %>%
    rename(parameter = .rep,
           prediction = .sim) %>%
    filter(datetime > reference_date) %>%
    mutate(reference_datetime = as_datetime(reference_date),
           family = "ensemble",
           model_id = "randomWalk",
           project_id = config$project_id,
           duration = config$target_groups$Coastal$group_vars$chlora_cci$duration) %>%
    select(model_id, datetime, reference_datetime, site_id, family, parameter,
           variable, prediction, project_id, duration) %>%
    mutate(datetime = as_datetime(datetime),
           reference_datetime = as_datetime(reference_datetime))

  if (nrow(RW_forecasts_EFI) == 0) {
    message("No future forecasts for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  forecast_file <- paste("coastal", reference_date, "randomWalk.csv.gz", sep = "-")
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
  message("Uploaded coastal random walk for ", reference_date)
}
