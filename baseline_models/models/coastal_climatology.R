## Climatology Null Model for Coastal Thrust
## Defined as a function + called by run_coastal_baselines.R

run_coastal_climatology <- function(reference_date, config, targets_all) {
  library(tidyverse)
  library(lubridate)
  library(aws.s3)
  library(imputeTS)

  reference_date <- as_date(reference_date)

  # Filter training data to <= reference date, CCI corrected only
  targets <- targets_all %>%
    filter(datetime <= reference_date,
           variable == "chlora_cci_corrected")

  if (nrow(targets) == 0) {
    message("No training data for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  # DOY climatology from all data up to reference_date
  target_clim <- targets %>%
    mutate(doy = yday(datetime)) %>%
    group_by(doy, site_id, variable) %>%
    summarise(mean_val = mean(observation, na.rm = TRUE),
              sd_val = sd(observation, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(mean_val = ifelse(is.nan(mean_val), NA, mean_val))

  horizon <- config$target_groups$Coastal$group_vars$chlora_cci$max_horizon
  start_date <- reference_date + days(1)
  forecast_dates <- seq(start_date, start_date + days(horizon - 1), by = "1 day")
  forecast_doy <- yday(forecast_dates)

  forecast_dates_df <- tibble(datetime = forecast_dates, doy = forecast_doy)

  # Build complete grid w/ every site x variable x forecast date, then join w/ climatology
  all_combos <- target_clim %>% distinct(site_id, variable)

  combined <- crossing(all_combos, forecast_dates_df) %>%
    left_join(target_clim %>% mutate(doy = as.integer(doy)),
              by = c("site_id", "variable", "doy")) %>%
    group_by(site_id, variable) %>%
    filter(sum(!is.na(mean_val)) >= 2) %>%
    mutate(
      mu    = imputeTS::na_interpolation(mean_val),
      # fall back to sd of climatology means when DOY sd is unavailable
      sigma = coalesce(median(sd_val, na.rm = TRUE), sd(mean_val, na.rm = TRUE))
    ) %>%
    pivot_longer(c("mu", "sigma"), names_to = "parameter", values_to = "prediction") %>%
    mutate(family = "normal") %>%
    ungroup() %>%
    mutate(reference_datetime = as_datetime(reference_date),
           model_id = "climatology",
           project_id = config$project_id,
           duration = config$target_groups$Coastal$group_vars$chlora_cci$duration) %>%
    select(model_id, datetime, reference_datetime, site_id, family, parameter,
           variable, prediction, project_id, duration)

  if (nrow(combined) == 0) {
    message("No forecast produced for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  forecast_file <- paste("coastal", reference_date, "climatology.csv.gz", sep = "-")
  write_csv(combined, forecast_file)

  aws.s3::put_object(
    file = forecast_file,
    object = paste0(config$forecasts_bucket, "/null-models/", forecast_file),
    bucket = config$s3_bucket_write,
    base_url = gsub("https://", "", config$endpoint),
    use_https = TRUE,
    region = ""
  )

  unlink(forecast_file)
  message("Uploaded coastal climatology for ", reference_date)
}
