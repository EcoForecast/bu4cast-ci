## Hourly random walk with backfill

RW_hourly_forecast <- function(site, var, h, reference_date,
                               boot_number = 31, ...) {
  reference_date <- lubridate::as_date(reference_date)
  reference_end <- lubridate::as_datetime(reference_date + lubridate::days(1))

  forecast_starts <- targets %>%
    dplyr::mutate(datetime = lubridate::floor_date(lubridate::as_datetime(datetime), "hour")) %>%
    dplyr::filter(!is.na(observation), site_id == site, variable == var) %>%
    dplyr::summarise(start_dt = max(datetime) + lubridate::hours(1)) %>%
    dplyr::mutate(
      start_dt = if_else(start_dt < reference_end - lubridate::hours(h), reference_end, start_dt),
      h_total = as.numeric(difftime(reference_end, start_dt, units = "hours")) + h
    )
# added cap to avoid forecasts too far into the future (was forecasting some sites 35 years into the future due to lack of recent data)
  
  message(site, " ", var, " hourly RW (ref=", reference_date, ")")

  targets_use <- targets %>%
    dplyr::mutate(
      datetime = lubridate::floor_date(lubridate::as_datetime(datetime), "hour"),
      observation = ifelse(observation <= 0, NA, observation),
      observation = ifelse(observation > 3 * quantile(observation, 0.99, na.rm = TRUE), NA, observation)
    ) %>%
    dplyr::filter(site_id == site, variable == var) %>%
    dplyr::group_by(datetime, site_id, variable) %>%
    dplyr::summarise(observation = mean(observation, na.rm = TRUE), .groups = "drop") %>%
    tsibble::as_tsibble(key = c("variable", "site_id"), index = "datetime") %>%
    tsibble::fill_gaps(.end = reference_end - lubridate::hours(1)) %>%
    dplyr::filter(datetime >= reference_end - lubridate::days(730),
                  datetime < forecast_starts$start_dt)

  if (nrow(targets_use) == 0 || sum(!is.na(targets_use$observation)) == 0 ||
      is.na(forecast_starts$h_total) || forecast_starts$h_total <= 0) {
    message("no targets available or h <= 0, skipping")
    return(data.frame(
      variable = character(), site_id = character(),
      .model = character(), datetime = lubridate::ymd_hms(),
      .rep = character(), .sim = numeric()
    ))
  }

  RW_model <- targets_use %>% fabletools::model(RW = fable::RW(observation))

  forecast <- tryCatch({
    RW_model %>% fabletools::generate(
      h = as.numeric(forecast_starts$h_total),
      bootstrap = TRUE,
      times = boot_number
    )
  }, error = function(e) {
    message("generate() failed for ", site, " ", var, ": ", e$message, ", skipping")
    return(data.frame(
      variable = character(), site_id = character(),
      .model = character(), datetime = lubridate::ymd_hms(),
      .rep = character(), .sim = numeric()
    ))
  })

  message("hourly forecast finished")
  return(forecast)
  }
