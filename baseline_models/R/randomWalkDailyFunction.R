## Daily random walk with backfill

RW_daily_forecast <- function(site, var, h, reference_date,
                                      boot_number = 31, ...) {
  reference_date <- lubridate::as_date(reference_date)

  forecast_starts <- targets %>%
    dplyr::mutate(datetime = lubridate::as_date(datetime)) %>%
    dplyr::filter(!is.na(observation), site_id == site, variable == var) %>%
    dplyr::summarise(start_date = max(datetime) + lubridate::days(1)) %>%
    dplyr::mutate(
      start_date = if_else(start_date < reference_date, reference_date, start_date),
      h_total = as.numeric(reference_date - start_date) + h + 1
    )

  message(site, " ", var, " daily RW (ref=", reference_date, ")")

  targets_use <- targets %>%
    dplyr::mutate(datetime = lubridate::as_date(datetime)) %>%
    dplyr::filter(site_id == site, variable == var) %>%
    dplyr::mutate(
      observation = ifelse(observation <= 0, NA, observation),
      observation = ifelse(observation > 3 * quantile(observation, 0.99, na.rm = TRUE), NA, observation)
    ) %>%
    dplyr::group_by(datetime, site_id, variable) %>%
    dplyr::summarise(observation = mean(observation, na.rm = TRUE), .groups = "drop") %>%
    tsibble::as_tsibble(key = c("variable", "site_id"), index = "datetime") %>%
    tsibble::fill_gaps(.end = reference_date) %>%
    dplyr::filter(datetime < forecast_starts$start_date)

  if (nrow(targets_use) == 0 || is.na(forecast_starts$h_total) || forecast_starts$h_total <= 0) {
    message("no data or h <= 0, skipping")
    return(data.frame(
      variable = character(), site_id = character(),
      .model = character(), datetime = lubridate::ymd(),
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
    .model = character(), datetime = lubridate::ymd(),
    .rep = character(), .sim = numeric()
  ))
})

message("daily forecast finished")
return(forecast)
