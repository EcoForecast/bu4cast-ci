## Climatology Null Model for Urban Thrust
# Called by run_urban_baselines.R

run_urban_climatology <- function(reference_date, config, targets_all, sites_metadata) {
  library(tidyverse)
  library(lubridate)
  library(aws.s3)
  library(imputeTS)

  reference_date <- as_date(reference_date)

  # Filter training data to <= reference_date, remove negatives and extreme outliers (urban target had some negative observations)
  urban_data <- targets_all %>%
    filter(datetime <= as_datetime(reference_date)) %>%
    group_by(variable) %>%
    mutate(observation = ifelse(observation <= 0, NA, observation),
           observation = ifelse(observation > 3 * quantile(observation, 0.99, na.rm = TRUE), NA, observation)) %>%
    ungroup() %>%
    mutate(doy = yday(datetime),
           hour_of_day = hour(datetime),
           year = year(datetime))

  if (nrow(urban_data) == 0) {
    message("No training data for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  # Active sites (tagged in target)
  last_year <- year(reference_date) - 1

  active_sites_for <- function(active_col, start_col) {
    sites_metadata %>%
      filter({{ active_col }} == TRUE &
               (yday({{ start_col }}) < yday(reference_date) |
                  year({{ start_col }}) < last_year)) %>%
      pull(field_site_id)
  }

  active_P1H_sites <- unique(c(
    active_sites_for(PM2.5_P1H_Active, PM2.5_P1H_StartDate),
    active_sites_for(PM10_P1H_Active, PM10_P1H_StartDate),
    active_sites_for(O3_Active, O3_StartDate),
    active_sites_for(NO2_P1H_Active, NO2_P1H_StartDate)
  ))

  active_P1D_sites <- unique(c(
    active_sites_for(PM2.5_P1D_Active, PM2.5_P1D_StartDate),
    active_sites_for(PM10_P1D_Active, PM10_P1D_StartDate)
  ))

  active_P1D_variables <- c("PM2.5_P1D", "PM10_P1D")
  active_P1H_variables <- c("PM2.5_P1H", "PM10_P1H", "NO2_P1H", "O3")

  # Climatology stats

  start_date     <- reference_date + days(1)
  end_date       <- reference_date + days(35)
  forecast_dates <- seq(as.POSIXct(start_date), as.POSIXct(end_date), by = "1 day")
  forecast_times <- seq(as.POSIXct(start_date), as.POSIXct(end_date), by = "hour")

  clim_daily <- urban_data %>%
    filter(duration == "P1D",
           site_id %in% active_P1D_sites,
           variable %in% active_P1D_variables) %>%
    group_by(site_id, doy, variable) %>%
    summarise(mean_value = mean(observation, na.rm = TRUE),
              sd_value = sd(observation, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(mean_value = ifelse(is.nan(mean_value), NA, mean_value))

  clim_hourly <- urban_data %>%
    filter(duration == "PT1H",
           site_id %in% active_P1H_sites,
           variable %in% active_P1H_variables) %>%
    group_by(site_id, doy, hour_of_day, variable) %>%
    summarise(mean_value = mean(observation, na.rm = TRUE),
              sd_value = sd(observation, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(mean_value = ifelse(is.nan(mean_value), NA, mean_value))

  # Forecast grids
  daily_grid <- expand.grid(
    site_id = active_P1D_sites,
    datetime = forecast_dates,
    variable = active_P1D_variables,
    stringsAsFactors = FALSE
  ) %>%
    mutate(datetime = as.POSIXct(datetime), doy = yday(datetime))

  combined_daily <- daily_grid %>%
    left_join(clim_daily, by = c("site_id", "doy", "variable")) %>%
    group_by(site_id, variable) %>%
    filter(sum(!is.na(mean_value)) >= 2) %>%
    mutate(mu    = imputeTS::na_interpolation(mean_value),
           sigma = coalesce(median(sd_value, na.rm = TRUE), sd(mean_value, na.rm = TRUE))) %>%
    pivot_longer(c("mu", "sigma"), names_to = "parameter", values_to = "prediction") %>%
    mutate(family = "normal", duration = "P1D") %>%
    ungroup()

  hourly_grid <- expand.grid(
    site_id = active_P1H_sites,
    datetime = forecast_times,
    variable = active_P1H_variables,
    stringsAsFactors = FALSE
  ) %>%
    mutate(datetime = as.POSIXct(datetime),
           doy = yday(datetime),
           hour_of_day = hour(datetime))

  combined_hourly <- hourly_grid %>%
    left_join(clim_hourly, by = c("site_id", "doy", "hour_of_day", "variable")) %>%
    group_by(site_id, variable) %>%
    filter(sum(!is.na(mean_value)) >= 2) %>%
    mutate(mu    = imputeTS::na_interpolation(mean_value),
           sigma = coalesce(median(sd_value, na.rm = TRUE), sd(mean_value, na.rm = TRUE))) %>%
    pivot_longer(c("mu", "sigma"), names_to = "parameter", values_to = "prediction") %>%
    mutate(family = "normal", duration = "PT1H") %>%
    ungroup()

  # Format/upload
  combined <- bind_rows(combined_daily, combined_hourly) %>%
    mutate(reference_datetime = as_datetime(reference_date),
           model_id = "climatology",
           project_id = config$project_id) %>%
    select(model_id, datetime, reference_datetime, site_id, family, parameter,
           variable, prediction, project_id, duration)

  if (nrow(combined) == 0) {
    message("No forecast produced for ", reference_date, ", skipping")
    return(invisible(NULL))
  }

  forecast_file <- paste("urban", reference_date, "climatology.csv.gz", sep = "-")
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
  message("Uploaded urban climatology for ", reference_date)
}
