## Coastal Targets Script for BU FRP Coastal
## Downloads buoy and CCI chlorophyll-a data, formats to standard, writes to S3
## Author: Cami Webb, cwebb16@bu.edu
## Created: 02-18-2025

library(rerddap)
library(arrow)
library(dplyr)
library(lubridate)
library(foreach)
library(doParallel)
library(doSNOW)
library(ncdf4)
library(jsonlite)
library(RCurl)
library(tidyr)
library(yaml)

progress_msg <- function(label, i, n) {
  message(sprintf("[%s] %d / %d", label, i, n))
}

config <- yaml::read_yaml("challenge_configuration.yaml")

## Configuration

challenge_name <- config$target_groups$Coastal$target_name
project_id     <- config$project_id
filename       <- config$target_groups$Coastal$targets_filepath

print(paste("challenge_name:", challenge_name))
print(paste("filename:", filename))

# Buoy location (previously calculated mean lat/lon of buoy, need for CCI box)
buoy_lat <- config$target_groups$Coastal$buoy_lat
buoy_lon <- config$target_groups$Coastal$buoy_lon

s3 <- arrow::s3_bucket(
  config$s3_bucket_read,
  endpoint_override = config$endpoint,
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

old_data <- tryCatch(
  arrow::read_csv_arrow(s3$path(filename)) %>% as.data.frame(),
  error = function(e) {
    message("No existing targets file found. Creating a new one.")
    NULL
  }
)

buoy_site_id <- "1"

# Full history from 2006
start_date_buoy  <- as.Date("2006-01-01")
start_date_cci   <- as.Date("2006-01-01")

end_date <- as.Date(Sys.Date() - 1)  # yesterday

if (!is.null(old_data) && nrow(old_data) > 0) {

  old_buoy  <- old_data[old_data$variable == "chlora_buoy",  , drop = FALSE]
  old_cci   <- old_data[old_data$variable == "chlora_cci",   , drop = FALSE]

  if (nrow(old_buoy) > 0) {
    last_buoy_date <- max(as.Date(substr(old_buoy$datetime, 1, 10)), na.rm = TRUE)
    if (is.finite(last_buoy_date)) start_date_buoy <- last_buoy_date + 1
  }

  if (nrow(old_cci) > 0) {
    last_cci_date <- max(as.Date(substr(old_cci$datetime, 1, 10)), na.rm = TRUE)
    if (is.finite(last_cci_date)) start_date_cci <- last_cci_date + 1
  }
}

# If already up-to-date, skip downloads
if (start_date_buoy  > end_date) message("Buoy already up-to-date; no buoy download needed.")
if (start_date_cci   > end_date) message("CCI already up-to-date; no CCI download needed.")

# Convert to character to be safe
start_date_buoy_chr  <- as.character(start_date_buoy)
end_date_chr         <- as.character(end_date)

## Download buoy data

message("Downloading buoy data from ERDDAP...")

get_buoy_data <- function(start_date, end_date) {
  buoy <- tabledap(
    'UNH_WBD',
    url = 'https://data.neracoos.org/erddap',
    fields = c('time', 'latitude', 'longitude', 'chlorophyll', 'temperature', 'turbidity', 'wspd', 'wdir', 'airtemp', 'airpress', 'oxygen'),
    paste0('time>=', start_date, 'T00:00:00Z'),
    paste0('time<=', end_date, 'T23:59:59Z')
  )

  # Clean up
  buoy$datetime <- as.POSIXct(buoy$time, format = "%Y-%m-%dT%H:%M:%SZ", tz = "GMT")
  buoy$date <- as.Date(buoy$datetime)

  # Convert to numeric
  buoy$chlorophyll <- as.numeric(buoy$chlorophyll)
  buoy$temperature <- as.numeric(buoy$temperature)
  buoy$turbidity <- as.numeric(buoy$turbidity)
  buoy$oxygen <- as.numeric(buoy$oxygen)

  return(buoy)
}

# Download data
if (start_date_buoy <= end_date) {
  progress_msg("BUOY download", 0, 1)
  buoy_data <- get_buoy_data(start_date_buoy_chr, end_date_chr)
  progress_msg("BUOY download", 1, 1)
} else {
  buoy_data <- data.frame()
}

# Safeguard if there's no new buoy data
buoy_has_data <- nrow(buoy_data) > 0
if (!buoy_has_data) {
  message("No new buoy data; skipping buoy processing.")
} else {

  # Filter out bad coordinates (was getting longitudes of 10^20+)
  buoy_data$latitude  <- as.numeric(buoy_data$latitude)
  buoy_data$longitude <- as.numeric(buoy_data$longitude)

  buoy_data_clean <- buoy_data %>%
    dplyr::filter(longitude > -180 & longitude < 180) %>%
    dplyr::filter(latitude > -90 & latitude < 90) %>%
    dplyr::filter(!is.na(latitude) & !is.na(longitude))

  if (nrow(buoy_data_clean) == 0) {
    message("Buoy returned rows but none had valid coords; skipping buoy processing.")
    buoy_has_data <- FALSE
  } else {

    # Calculate buoy location for CCI download
    buoy_lat <- mean(buoy_data_clean$latitude, na.rm = TRUE)
    buoy_lon <- mean(buoy_data_clean$longitude, na.rm = TRUE)

    buoy_daily <- buoy_data_clean %>%
      dplyr::mutate(date = as.Date(datetime)) %>%
      dplyr::group_by(date) %>%
      dplyr::summarise(
        latitude    = mean(latitude, na.rm = TRUE),
        longitude   = mean(longitude, na.rm = TRUE),
        chlorophyll = mean(chlorophyll, na.rm = TRUE),
        temperature = mean(temperature, na.rm = TRUE),
        turbidity   = mean(turbidity, na.rm = TRUE),
        wspd        = mean(wspd, na.rm = TRUE),
        wdir        = mean(wdir, na.rm = TRUE),
        airtemp     = mean(airtemp, na.rm = TRUE),
        airpress    = mean(airpress, na.rm = TRUE),
        oxygen      = mean(oxygen, na.rm = TRUE),
        n_obs       = n(),
        .groups = "drop"
      )

    buoy_data <- buoy_daily
    rm(buoy_daily)
  }
}

## Download OC_CCI data (https://rsg.pml.ac.uk/thredds/catalog-cci.html -> CCI_ALL-v6.0-1km-DAILY)

message("Downloading OC-CCI data...")

# 1km dataset
occci_dataset_id <- "CCI_ALL-v6.0-1km-DAILY"
occci_ncss_base  <- paste0("https://rsg.pml.ac.uk/thredds/ncss/grid/", occci_dataset_id)

# Variables (just chlor for this dataset)
occci_vars_wanted <- c("chlor_a")

# avoid 60s timeout, I set for 24 hours just to be safe for backfill
options(timeout = 60 * 60 * 24)

# start and end date
start_date_occci <- as.Date(start_date_cci)
end_date_occci   <- as.Date(end_date)

if (!is.null(old_data) && nrow(old_data) > 0) {
  old_occci <- old_data[old_data$variable == "chlora_cci", , drop = FALSE]
  if (nrow(old_occci) > 0) {
    last_occci_date <- max(as.Date(substr(old_occci$datetime, 1, 10)), na.rm = TRUE)
    if (is.finite(last_occci_date)) start_date_occci <- last_occci_date + 1
  }
}

if (start_date_occci > end_date_occci) {
  message("No new CCI data; skipping CCI processing.")
  cci_data <- data.frame()
  k_cci <- 0L
} else {

  # Here I'm using a larger download box than 5x5 just to center the 5x5 better (later I slice the 5x5 exactly)
  # 1 km ~ 1/111 degree lat. Use ~4 km halfwidth to get >= 5x5 coverage
  # This could be stupid and redundant but I'm trying to be cautious (for once)
  deg_per_km_lat <- 1 / 111
  half_km_bbox   <- 4

  lat_half <- half_km_bbox * deg_per_km_lat
  lon_half <- lat_half / cos(buoy_lat * pi / 180)

  north <- buoy_lat + lat_half
  south <- buoy_lat - lat_half
  east  <- buoy_lon + lon_half
  west  <- buoy_lon - lon_half

  # Only process dates not in the coastal-targets.csv
  existing_occci_dates <- as.Date(character(0))

  if (!is.null(old_data) && nrow(old_data) > 0) {
    old_occci <- old_data[old_data$variable == "chlora_cci", , drop = FALSE]
    if (nrow(old_occci) > 0) {
      existing_occci_dates <- unique(as.Date(substr(old_occci$datetime, 1, 10)))
      existing_occci_dates <- existing_occci_dates[!is.na(existing_occci_dates)]
    }
  }

  days_all <- seq.Date(start_date_occci, end_date_occci, by = "day")
  days <- days_all[!(days_all %in% existing_occci_dates)]

  message("OC-CCI new dates to process: ", length(days),
          if (length(days) > 0) paste0(" (", min(days), " to ", max(days), ")") else "")
total_days <- length(days)
progress_msg("CCI download", 0, total_days)
          
  if (length(days) == 0) {
    message("No new OC-CCI dates to process; skipping.")
    cci_data <- data.frame()
    k_cci <- 0L
  } else {

    # Trim 1km 5x5 pixel box centered around closest pixel to buoy
    extract_day_5x5 <- function(day, vars) {
      if (length(vars) == 0) return(NULL)

      url <- paste0(
        occci_ncss_base, "?",
        "var=", paste(vars, collapse = "&var="),
        "&north=", north, "&south=", south, "&east=", east, "&west=", west,
        "&time_start=", as.character(day), "T00:00:00Z",
        "&time_end=",   as.character(day), "T23:59:59Z",
        "&accept=netcdf"
      )

      tmp <- tempfile(fileext = ".nc")
      ok <- tryCatch({
        suppressWarnings(utils::download.file(url, destfile = tmp, mode = "wb", quiet = TRUE))
        TRUE
      }, warning = function(w) FALSE,
         error   = function(e) FALSE)

      if (!ok || !file.exists(tmp) || file.info(tmp)$size == 0) {
        if (file.exists(tmp)) unlink(tmp)
        return(NULL)
      }

      nc <- tryCatch(ncdf4::nc_open(tmp), error = function(e) {
        message("nc_open failed for ", as.character(day), ": ", e$message)
        NULL
      })
      if (is.null(nc)) { unlink(tmp); return(NULL) }
      on.exit({
        try(ncdf4::nc_close(nc), silent = TRUE)
        unlink(tmp)
      }, add = TRUE)

      lat <- tryCatch(as.numeric(ncdf4::ncvar_get(nc, "lat")), error = function(e) NULL)
      lon <- tryCatch(as.numeric(ncdf4::ncvar_get(nc, "lon")), error = function(e) NULL)
      if (is.null(lat) || is.null(lon)) return(NULL)

      # Nearest pixel indices
      i0 <- which.min((lat - buoy_lat)^2)  # lat index
      j0 <- which.min((lon - buoy_lon)^2)  # lon index

      # 5x5 bounds
      i1 <- max(1, i0 - 2); i2 <- min(length(lat), i0 + 2)
      j1 <- max(1, j0 - 2); j2 <- min(length(lon), j0 + 2)

      out <- list(date = day)

      for (v in vars) {
        arr <- tryCatch(ncdf4::ncvar_get(nc, v), error = function(e) NULL)
        if (is.null(arr)) next

        # Drop time dim if present
        if (length(dim(arr)) == 3) arr <- arr[,,1, drop = TRUE]

        d <- dim(arr)
        if (is.null(d) || length(d) != 2) next

        # decide whether arr is lon,lat or lat,lon (has been inconsistent so this is just a safeguard)
        if (d[1] == length(lon) && d[2] == length(lat)) {
          win <- arr[j1:j2, i1:i2, drop = FALSE]  # lon,lat
        } else if (d[1] == length(lat) && d[2] == length(lon)) {
          win <- arr[i1:i2, j1:j2, drop = FALSE]  # lat,lon
        } else {
          next
        }

        vals <- as.vector(win)
        vals <- vals[is.finite(vals)]

        out[[paste0(v, "_mean")]] <- if (length(vals) > 0) mean(vals) else NA_real_
        out[[paste0(v, "_sd")]]   <- if (length(vals) > 1) sd(vals) else NA_real_
        out[[paste0(v, "_n")]]    <- length(vals)  # = 25 if all pixels are valid
      }

      as.data.frame(out, stringsAsFactors = FALSE)
    }

    message("OC-CCI rows to process: ", length(days))

    out_list <- vector("list", length(days))
    kk <- 0L

    for (ii in seq_along(days)) {
  progress_msg("CCI download", ii, total_days)
  res <- tryCatch(extract_day_5x5(days[ii], occci_vars_wanted),
                  error = function(e) { message("extract failed: ", e$message); NULL })
  if (!is.null(res)) {
    kk <- kk + 1L
    out_list[[kk]] <- res
  }
}
  progress_msg("CCI download", total_days, total_days)

    occci_df <- if (kk == 0L) data.frame() else do.call(rbind, out_list[seq_len(kk)])
    if (nrow(occci_df) > 0) {
      occci_df$date <- as.Date(occci_df$date)
      occci_df <- occci_df %>% dplyr::arrange(date)
    }

    # Match downstream formatting
    if (nrow(occci_df) == 0) {
      cci_data <- data.frame()
      k_cci <- 0L
    } else {
      cci_data <- occci_df %>%
        dplyr::transmute(
          date = date,
          chlorophyll_mean = if ("chlor_a_mean" %in% names(.)) chlor_a_mean else NA_real_,
          chlorophyll_sd   = if ("chlor_a_sd"   %in% names(.)) chlor_a_sd   else NA_real_,
          chlorophyll_n    = if ("chlor_a_n"    %in% names(.)) chlor_a_n    else NA_real_
        )
      k_cci <- nrow(cci_data)
    }

    message("OC-CCI rows processed: ", k_cci)

    if (exists("cci_data") && nrow(cci_data) > 0) {
      valid_n <- cci_data %>%
        dplyr::filter(
          !is.na(chlorophyll_mean),
          chlorophyll_n > 0
        ) %>%
        dplyr::pull(chlorophyll_n)
      # Just to monitor
      message("Mean n = ",   mean(valid_n,   na.rm = TRUE))
      message("Median n = ", median(valid_n, na.rm = TRUE))
    }
  }
}
          
## Format
message("Formatting to standard format...")

# Always return something even if empty
empty_targets <- function() {
  tibble::tibble(
    project_id  = character(),
    site_id     = character(),
    datetime    = character(),
    duration    = character(),
    variable    = character(),
    observation = numeric()
  )
}

to_standard_chlora <- function(df, site_id, mode, duration = "P1D") {
  df %>%
    dplyr::transmute(
      project_id  = project_id,
      site_id     = as.character(site_id),
      datetime    = as.character(date),
      duration    = duration,
      variable    = paste0("chlora_", mode),
      observation = as.numeric(chlorophyll)
    ) %>%
    dplyr::filter(!is.na(observation))
}

# Buoy formatted or empty
if (!exists("buoy_has_data") || !isTRUE(buoy_has_data)) {
  message("No new buoy observations; buoy_formatted will be empty.")
  buoy_formatted <- empty_targets()
} else {
  buoy_formatted <- buoy_data %>%
    dplyr::select(date, chlorophyll) %>%
    dplyr::filter(chlorophyll > 0.001) %>%
    to_standard_chlora(site_id = buoy_site_id, mode = "buoy")
}

# CCI formatted or empty
if (!exists("k_cci") || is.na(k_cci) || k_cci == 0) {
  message("No usable, new CCI observations; cci_formatted will be empty.")
  cci_formatted <- empty_targets()
} else {

  cci_daily <- cci_data %>%
    dplyr::transmute(
      date        = as.Date(date),
      chlorophyll = as.numeric(chlorophyll_mean)
    ) %>%
    dplyr::mutate(
      chlorophyll = dplyr::if_else(is.nan(chlorophyll), NA_real_, chlorophyll)
    )

  # filter exclude dates and minimum buoy chlorophyll
  cci_daily <- cci_daily %>%
    dplyr::filter(!date %in% exclude_dates)

  cci_formatted <- cci_daily %>%
    to_standard_chlora(site_id = buoy_site_id, mode = "cci")
}
          
# Combine
all_targets <- dplyr::bind_rows(buoy_formatted, cci_formatted)

## Append to existing data

data <- all_targets %>%
  mutate(
    project_id  = as.character(project_id),
    site_id     = as.character(site_id),
    datetime    = as.character(datetime),
    duration    = as.character(duration),
    variable    = as.character(variable),
    observation = as.numeric(observation)
  )

primary_keys <- c("project_id", "site_id", "datetime", "duration", "variable")

# De-dupe today's run just in case
data <- data %>%
  distinct(across(all_of(primary_keys)), .keep_all = TRUE)

if (!is.null(old_data) && nrow(old_data) > 0) {

  old_data <- old_data %>%
    mutate(
      project_id  = as.character(project_id),
      site_id     = as.character(site_id),
      datetime    = as.character(datetime),
      duration    = as.character(duration),
      variable    = as.character(variable),
      observation = as.numeric(observation)
    ) %>%
    distinct(across(all_of(primary_keys)), .keep_all = TRUE)

  # Keep old rows that are not being replaced, then add today's rows
  new_data <- old_data %>%
    anti_join(data, by = primary_keys) %>%
    bind_rows(data)

} else {
  new_data <- data
}

new_data <- new_data %>%
  arrange(site_id, datetime, variable)

message(paste0("Rows in old data: ", ifelse(is.null(old_data), 0, nrow(old_data))))
message(paste0("Rows of new data: ", nrow(data)))
message(paste0("Rows in appended data: ", nrow(new_data)))

if (nrow(all_targets) == 0) {
  message("No new targets today; skipping write")
} else {
  message("Writing updated targets back to S3...")
  arrow::write_csv_arrow(new_data, sink = s3$path(filename))
}


message("Pinging health check...")
tryCatch(
  RCurl::getURL(config$target_groups$Coastal$health_check_url),
  error = function(e) message("Health check ping failed: ", e$message)
)

message("Coastal targets script complete!")
