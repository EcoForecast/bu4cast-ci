## Coastal Targets Script for BU FRP Coastal
## Downloads buoy, MODIS, and CCI data, formats to standard, writes to S3
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

progress_msg <- function(label, i, n) {
  message(sprintf("[%s] %d / %d", label, i, n))
}

# Source NASA download function (Author: Dongchen Zhang)
source("targets/R/NASA_DAAC_download.R")

## Configuration

challenge_name <- "coastal"
project_id <- "bu4cast"

# Create file name/folder
filename = paste("challenges/targets/project_id=bu4cast/", challenge_name,
                 "-targets.csv", sep = "")

print(paste("challenge_name:", challenge_name))
print(paste("filename:", filename))

# Buoy location (previously calculated mean lat/lon of buoy, need for CCI box)
buoy_lat <- 43.022942490079
buoy_lon <- -70.5475341233827

# Read old data already in s3 bucket
s3_read <- arrow::s3_bucket(
  "bu4cast-ci-read",
  endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
  access_key = Sys.getenv("OSN_KEY"),
  secret_key = Sys.getenv("OSN_SECRET"),
  scheme = "https"
)

old_data <- tryCatch(
  arrow::read_csv_arrow(s3_read$path(filename)) %>% as.data.frame(),
  error = function(e) {
    message("No existing targets file found. Creating a new one.")
    NULL
  }
)

# Full history from 2006
start_date_buoy  <- as.Date("2006-01-01")
start_date_modis <- as.Date("2006-01-01")
start_date_cci   <- as.Date("2006-01-01")

end_date <- as.Date(Sys.Date() - 1)  # yesterday

if (!is.null(old_data) && nrow(old_data) > 0) {

  old_buoy  <- old_data[old_data$site_id == "UNH_buoy", , drop = FALSE]
  old_modis <- old_data[old_data$site_id == "MODIS",   , drop = FALSE]
  old_cci   <- old_data[old_data$site_id == "CCI",     , drop = FALSE]

  if (nrow(old_buoy) > 0) {
    last_buoy_date <- max(as.Date(substr(old_buoy$datetime, 1, 10)), na.rm = TRUE)
    if (is.finite(last_buoy_date)) start_date_buoy <- last_buoy_date + 1
  }

  if (nrow(old_modis) > 0) {
    last_modis_date <- max(as.Date(substr(old_modis$datetime, 1, 10)), na.rm = TRUE)
    if (is.finite(last_modis_date)) start_date_modis <- last_modis_date + 1
  }

  if (nrow(old_cci) > 0) {
    last_cci_date <- max(as.Date(substr(old_cci$datetime, 1, 10)), na.rm = TRUE)
    if (is.finite(last_cci_date)) start_date_cci <- last_cci_date + 1
  }
}

# If already up-to-date, skip downloads
if (start_date_buoy  > end_date) message("Buoy already up-to-date; no buoy download needed.")
if (start_date_modis > end_date) message("MODIS already up-to-date; no MODIS download needed.")
if (start_date_cci   > end_date) message("CCI already up-to-date; no CCI download needed.")

# Convert to character to be safe
start_date_buoy_chr  <- as.character(start_date_buoy)
start_date_modis_chr <- as.character(start_date_modis)
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

## Download MODIS Aqua data

message("Downloading MODIS-Aqua Level 2 OC data...")

# Calculate bounding box for 5x5 pixels at 1km
box_size_deg <- (1 * 5 * 0.04) / 2

ul_lat <- buoy_lat + box_size_deg
lr_lat <- buoy_lat - box_size_deg
ul_lon <- buoy_lon - box_size_deg
lr_lon <- buoy_lon + box_size_deg

# DOI for Level 2 OC
modis_doi <- "10.5067/AQUA/MODIS/L2/OC/2022.0"

# Set up temp download directory
modis_dir <- file.path(tempdir(), "modis")
dir.create(modis_dir, showWarnings = FALSE, recursive = TRUE)

if (as.Date(start_date_modis_chr) > as.Date(end_date_chr)) {
  message("MODIS already up-to-date; skipping MODIS download.")
  modis_files <- character(0)
} else {

  # Get all URLs to know total count
  all_urls <- NASA_DAAC_download(
    ul_lat = ul_lat,
    ul_lon = ul_lon,
    lr_lat = lr_lat,
    lr_lon = lr_lon,
    from = start_date_modis_chr,
    to   = end_date_chr,
    doi  = modis_doi,
    just_path = TRUE
  )

  total_files <- length(all_urls)
  message(paste("Found", total_files, "new files to download"))

  # cores 
  ncore <- max(1, min(1, parallel::detectCores() - 1, total_files))

  progress_msg("MODIS download", 0, total_files)

  # Download
  message("Starting MODIS Aqua downloads...")
  modis_files <- NASA_DAAC_download(
    ul_lat = ul_lat,
    ul_lon = ul_lon,
    lr_lat = lr_lat,
    lr_lon = lr_lon,
    from = start_date_modis_chr,
    to   = end_date_chr,
    doi = modis_doi,
    outdir = modis_dir,
    credential_path = "~/.netrc",
    ncore = ncore,
    just_path = FALSE
  )
  
  progress_msg("MODIS download", length(modis_files), total_files)

  # If download returned NA (all files already exist)
 if (length(modis_files) == 1 && is.na(modis_files)) {
  modis_files <- list.files(modis_dir, pattern = "\\.nc$", full.names = TRUE)
}

message(paste("Successfully downloaded", length(modis_files), "files"))
  }

## Process MODIS data

message("Processing MODIS 5x5 pixel boxes...")

process_modis <- function(ncfile, buoy_lon, buoy_lat) {

  nc <- nc_open(ncfile)
  on.exit(nc_close(nc), add = TRUE)

  chl <- ncvar_get(nc, "geophysical_data/chlor_a")
  kd  <- ncvar_get(nc, "geophysical_data/Kd_490")
  l2f <- ncvar_get(nc, "geophysical_data/l2_flags")
  poc <- ncvar_get(nc, "geophysical_data/poc")
  pic <- ncvar_get(nc, "geophysical_data/pic")
  lon <- ncvar_get(nc, "navigation_data/longitude")
  lat <- ncvar_get(nc, "navigation_data/latitude")

  # Get rid of negative/fill values
  chl[chl < 0] <- NA
  kd[kd < 0] <- NA
  poc[poc < 0] <- NA
  pic[pic < 0] <- NA

  # Calculate center of 5x5 box
  d2 <- (lon - buoy_lon)^2 + (lat - buoy_lat)^2 # compute distance to buoy for each pixel
  idx <- which.min(d2) # index of closest pixel
  ij  <- arrayInd(idx, dim(chl))  # convert to row+column
  r0 <- ij[1]; c0 <- ij[2] # center of 5x5 box

  # 5x5 window bounds (centered on pixel closest to buoy)
  r1 <- max(1, r0 - 2); r2 <- min(nrow(chl), r0 + 2)
  c1 <- max(1, c0 - 2); c2 <- min(ncol(chl), c0 + 2)

  l2_flags <- l2f[r1:r2, c1:c2] # matrix of flags in box
  l2_flags_json <- jsonlite::toJSON(l2_flags, auto_unbox = TRUE)

  # 5x5 windows
  chl_vals <- as.vector(chl[r1:r2, c1:c2])
  kd_vals  <- as.vector(kd[r1:r2,  c1:c2])
  poc_vals  <- as.vector(poc[r1:r2,  c1:c2])
  pic_vals  <- as.vector(pic[r1:r2,  c1:c2])

  chl_vals <- chl_vals[is.finite(chl_vals)]
  kd_vals  <- kd_vals[is.finite(kd_vals)]
  poc_vals  <- poc_vals[is.finite(poc_vals)]
  pic_vals  <- pic_vals[is.finite(pic_vals)]

  # if all empty, skip
  if (length(chl_vals) == 0 && length(kd_vals) == 0 && length(poc_vals) == 0 && length(pic_vals) == 0) {
    return(NULL)
  }

  # Get date from filename
  m <- regmatches(basename(ncfile), regexpr("\\.\\d{8}T\\d{6}", basename(ncfile)))
  if (length(m) == 0) return(NULL)
  file_date <- as.Date(substr(m, 2, 9), format = "%Y%m%d")

  data.frame(
    date = file_date,
    chlorophyll_mean = mean(chl_vals),
    chlorophyll_sd   = if (length(chl_vals) > 1) sd(chl_vals) else NA_real_,
    chlorophyll_n    = length(chl_vals),

    kd490_mean = if (length(kd_vals) > 0) mean(kd_vals) else NA_real_,
    kd490_sd   = if (length(kd_vals) > 1) sd(kd_vals) else NA_real_,
    kd490_n    = length(kd_vals),

    poc_mean = if (length(poc_vals) > 0) mean(poc_vals) else NA_real_,
    poc_sd   = if (length(poc_vals) > 1) sd(poc_vals) else NA_real_,
    poc_n    = length(poc_vals),

    pic_mean = if (length(pic_vals) > 0) mean(pic_vals) else NA_real_,
    pic_sd   = if (length(pic_vals) > 1) sd(pic_vals) else NA_real_,
    pic_n    = length(pic_vals),

    l2_flags = l2_flags_json,   # flags
    file = basename(ncfile),
    stringsAsFactors = FALSE
  )
}

# Check which dates exist already
existing_dates <- as.Date(character(0))
if (exists("old_data") && !is.null(old_data)) {
  old_df <- as.data.frame(old_data)
  old_modis <- old_df[old_df$site_id == "MODIS", , drop = FALSE]
  if (nrow(old_modis) > 0) {
    existing_dates <- unique(as.Date(substr(old_modis$datetime, 1, 10)))
    existing_dates <- existing_dates[!is.na(existing_dates)]
  }
}

# Only process new files/dates
file_dates <- vapply(modis_files, function(fp) {
  m <- regmatches(basename(fp), regexpr("\\.\\d{8}T\\d{6}", basename(fp)))
  if (length(m) == 0) return(NA_character_)
  as.character(as.Date(substr(m, 2, 9), format = "%Y%m%d"))
}, character(1))
file_dates <- as.Date(file_dates)

to_process <- modis_files[is.na(file_dates) | !(file_dates %in% existing_dates)]
message("MODIS files: ", length(modis_files), " | to process: ", length(to_process))

# loop to process
out <- vector("list", length(to_process))
k_modis <- 0L

for (i in seq_along(to_process)) {
  res <- tryCatch(
    process_modis(to_process[i], buoy_lon, buoy_lat),
    error = function(e) {
      message("MODIS failed: ", basename(to_process[i]), " :: ", e$message)
      NULL
    }
  )

  if (!is.null(res)) {
    k_modis <- k_modis + 1L
    out[[k_modis]] <- res
  }

  if (i %% 50 == 0) gc()
}

# Trim
out <- out[seq_len(k_modis)]

# Make df for MODIS data
modis_data <- if (k_modis == 0) data.frame() else do.call(rbind, out[seq_len(k_modis)])

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
  old_occci <- old_data[old_data$site_id == "CCI", , drop = FALSE]
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
    old_occci <- old_data[old_data$site_id == "CCI", , drop = FALSE]
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
      }, warning = function(w) {
        FALSE
      }, error = function(e) {
        FALSE
      })

      if (!ok || !file.exists(tmp) || file.info(tmp)$size == 0) {
        if (file.exists(tmp)) unlink(tmp)
        return(NULL)
      }

      nc <- tryCatch(ncdf4::nc_open(tmp), error = function(e) {
        message("nc_open failed for ", as.character(day), ": ", e$message)
        NULL
      })
      if (is.null(nc)) {
        unlink(tmp)
        return(NULL)
      }
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
        if (length(dim(arr)) == 3) {
          arr <- arr[,,1, drop = TRUE]
        }

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

      mean_n   <- mean(valid_n, na.rm = TRUE)
      median_n <- median(valid_n, na.rm = TRUE)

      # Just to monitor
      message("Mean n = ", mean_n)
      message("Median n = ", median_n)
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

to_standard_long <- function(df, site_prefix, duration = "P1D") {
  df %>%
    tidyr::pivot_longer(
      cols = -date,
      names_to = "variable",
      values_to = "observation"
    ) %>%
    dplyr::mutate(
      project_id = project_id,
      site_id    = paste0(site_prefix),
      datetime   = as.character(date),
      duration   = duration
    ) %>%
    dplyr::select(project_id, site_id, datetime, duration, variable, observation) %>%
    dplyr::filter(!is.na(observation))
}

# Buoy formatted or empty
if (!exists("buoy_has_data") || !isTRUE(buoy_has_data)) {
  message("No new buoy observations; buoy_formatted will be empty.")
  buoy_formatted <- empty_targets()
} else {
  buoy_formatted <- buoy_data %>%
    dplyr::select(date, chlorophyll, temperature, turbidity, oxygen, wspd, wdir, airtemp, airpress) %>%
    to_standard_long(site_prefix = "UNH_buoy")
}

# MODIS formatted or empty
if (!exists("modis_data") || is.null(modis_data) || nrow(modis_data) == 0) {
  message("No usable MODIS observations; modis_formatted will be empty.")
  modis_formatted <- empty_targets()
} else {

  # collapse multiple granules per day
  modis_daily <- modis_data %>%
    dplyr::group_by(date) %>%
    dplyr::summarise(
      chlorophyll = mean(chlorophyll_mean, na.rm = TRUE),
      kd_490      = mean(kd490_mean,       na.rm = TRUE),
      poc         = mean(poc_mean,         na.rm = TRUE),
      pic         = mean(pic_mean,         na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(across(c(chlorophyll, kd_490, poc, pic),
                         ~ dplyr::if_else(is.nan(.x), NA_real_, .x)))

  modis_formatted <- modis_daily %>%
    to_standard_long(site_prefix = "MODIS")
}

# CCI formatted or empty
if (!exists("k_cci") || is.na(k_cci) || k_cci == 0) {
  message("No usable, new CCI observations; cci_formatted will be empty.")
  cci_formatted <- empty_targets()
} else {

  cci_daily <- cci_data %>%
    dplyr::transmute(
      date = as.Date(date),
      chlorophyll = as.numeric(chlorophyll_mean)
    ) %>%
    dplyr::mutate(
      chlorophyll = dplyr::if_else(is.nan(chlorophyll), NA_real_, chlorophyll)
    )

  cci_formatted <- cci_daily %>%
    to_standard_long(site_prefix = "CCI")
}

# Combine
all_targets <- dplyr::bind_rows(buoy_formatted, modis_formatted, cci_formatted)

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
  message("No new buoy or CCI observations today; skipping write")
} else {

  message("Writing updated targets back to S3...")
  arrow::write_csv_arrow(new_data, sink = s3_read$path(filename))

}

if (exists("modis_files")) {
  suppressWarnings(try(unlink(modis_files), silent = TRUE))
}

message("Pinging health check...")
tryCatch(
  RCurl::getURL("https://hc-ping.com/af0bdaf6-3ec8-434b-bafd-d8fecc0508af"),
  error = function(e) message("Health check ping failed: ", e$message)
)

message("Coastal targets script complete!")
