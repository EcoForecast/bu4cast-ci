library(neon4cast) #  remotes::install_github("eco4cast/neon4cast")

library(readr)
library(dplyr)
library(arrow)
library(glue)
library(here)
library(minioclient)
library(tools)
library(fs)
library(stringr)
library(lubridate)
library(duckdb)
library(DBI)
source("submission_processing/forecast_output_validator.R")


install_mc()

# Read in configurations

config <- yaml::read_yaml("challenge_configuration.yaml")

sites <- readr::read_csv(config$catalog_config$site_metadata_url, show_col_types = FALSE) |>
  select(field_site_id, latitude, longitude) |>
  rename(site_id = field_site_id)

# Set up minio connections
minioclient::mc_alias_set(config$s3_bucket_read,
                          config$submissions_endpoint,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

minioclient::mc_alias_set(config$s3_bucket_write,
                          config$submissions_endpoint,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))


message(paste0("Starting Processing Submissions ", Sys.time()))

# Create a local directory

local_dir <- file.path(here::here(), "submissions")
unlink(local_dir, recursive = TRUE)
fs::dir_create(local_dir)

message("Downloading forecasts ...")

# Download write bucket to local directory
# Safety net for no submissions
tryCatch(
  {
    minioclient::mc_cp(
      from = config$submissions_write_bucket,
      to   = local_dir,
      recursive = TRUE
    )
    message("Data Read In From S3")
  },
  error = function(e) {
    message("Reading Data Failed: ", conditionMessage(e))
    quit(status = 0)
  }
)

submissions <- fs::dir_ls(local_dir, recurse = TRUE, type = "file") # lists all files in local_dir
print('Submissions')
print(submissions)

submissions <- submissions[stringr::str_detect(submissions, "usgsrc4cast", negate = TRUE)] # filter usgsrc4cast files out 
submissions_filenames <- basename(submissions) # grab just the filename not full path

print('Filenames')
print(submissions_filenames)

if(length(submissions) > 0){

  # Prevent AWS connections
  
  Sys.unsetenv("AWS_DEFAULT_REGION")
  Sys.unsetenv("AWS_S3_ENDPOINT")
  Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

  # Connect to DuckDB - helps write to S3 bucket
  key_id   <- Sys.getenv("OSN_KEY", "")
  secret   <- Sys.getenv("OSN_SECRET", "")
  
  conn <- dbConnect(duckdb())
  DBI::dbExecute(conn, "INSTALL httpfs;")
  DBI::dbExecute(conn, "LOAD httpfs;")
  
  sql <- sprintf("
  CREATE OR REPLACE SECRET s3_minio_osn (
    TYPE S3,
    KEY_ID '%s',
    SECRET '%s',
    ENDPOINT 'https://minio-s3.apps.shift.nerc.mghpcc.org',
    REGION 'us-east-1',
    USE_SSL TRUE
  )
", key_id, secret)
  
  DBI::dbExecute(conn, sql)
  
  # duckdbfs::duckdb_secrets(
  #                        endpoint = config$endpoint,
  #                        key = Sys.getenv("OSN_KEY"),
  #                        secret = Sys.getenv("OSN_SECRET"))

  s3_read <- arrow::s3_bucket(config$s3_bucket_read,
                         endpoint_override = config$endpoint,
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  time_stamp <- format(Sys.time(), format = "%Y%m%d%H%M%S")

  # Process each submission
  
  for(i in 1:length(submissions)){
    
    print(submissions[i])

    curr_submission <- basename(submissions[i]) # grab submission name
    theme <-  stringr::str_split(curr_submission, "-")[[1]][1] # grab category
    file_name_model_id <-  stringr::str_split(tools::file_path_sans_ext(tools::file_path_sans_ext(curr_submission)), "-")[[1]][5] # Grab model_id
    file_name_reference_datetime <- lubridate::as_datetime(paste0(stringr::str_split(curr_submission, "-")[[1]][2:4], collapse = "-")) # Grab date of submission
    submission_dir <- dirname(submissions[i]) # grab submission directory
    print(curr_submission)

    # not_tg <- stringr::str_detect(curr_submission, "tg", negate = TRUE)
    not_tg <- TRUE

    print(file_name_reference_datetime)
    # Only read in correctly formatted filenames
    if((tools::file_ext(curr_submission) %in% c("gz", "csv", "nc")) & not_tg & !is.na(file_name_reference_datetime)){

      print('Filename format correct')
      
      # Check format of file itself (eco4cast)
      valid <- forecast_output_validator_bu4cast(file.path(local_dir, curr_submission), config)
      
      print(paste0("Is the submission valid:", valid))
      
      if(valid){
        
        # Pull out forecast
        fc <- read4cast::read_forecast(submissions[i])

        # Get current datetime
        pub_datetime <- strftime(Sys.time(), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

        # Clean duration
        if(!"duration" %in% names(fc)){
          if(theme == "terrestrial_30min"){
            fc <- fc |> dplyr::mutate(duration = "PT30M")
          }else if(theme %in% c("ticks","beetles")){
            fc <- fc |> dplyr::mutate(duration = "P1W")
          }else if(theme %in% c("aquatics","phenology","terrestrial_daily")){
            fc <- fc |> dplyr::mutate(duration = "P1D")
          }else{
            if(stringr::str_detect(fc$datetime[1], ":")){
              fc <- fc |> dplyr::mutate(duration = "P1H")
            }else{
              fc <- fc |> dplyr::mutate(duration = "P1D")
            }
          }
        }

        fc <- fc |>
          mutate(duration = ifelse(duration == "PT30", "PT30M", duration))

        # Filter out horizons that are too long
        # Could include in config if possible
        fc <- fc |>
          mutate(horizon = as.integer(as.POSIXct(datetime) - as.POSIXct(reference_datetime))/ (60*60*24),
                 max_horizon = 35) |>
          filter(horizon <= max_horizon) |>
          select(-horizon, -max_horizon)

        # Switch ensemble to sample
        fc <- fc |>
          mutate(family = ifelse(family == "ensemble", "sample", family))

        # If needed, add/fill model id column
        if(!("model_id" %in% colnames(fc))){
          fc <- fc |> mutate(model_id = file_name_model_id)
        }else if(fc$model_id[1] == "null"){
          fc <- fc |> mutate(model_id = file_name_model_id)
        }

        # If needed, add reference_datetime
        if(!("reference_datetime" %in% colnames(fc))){
          fc <- fc |> mutate(reference_datetime = file_name_reference_datetime)
        }

        # Set column types and project id
        fc <- fc |>
          dplyr::mutate(pub_datetime = lubridate::as_datetime(pub_datetime),
                        datetime = lubridate::as_datetime(datetime),
                        reference_datetime = lubridate::as_datetime(reference_datetime),
                        reference_date = lubridate::as_date(reference_datetime),
                        parameter = as.character(parameter),
                        project_id = config$project_id) |>
          dplyr::filter(datetime >= reference_datetime)

        print(head(fc))
        
        # Add in a parquet for the read bucket
        s3_read$CreateDir(paste0("parquet/"))

        ## arrow write has gone nuts... let's update
        # Using duckdbfs
        duckdbfs::duckdb_s3_config(
          s3_endpoint = config$submissions_endpoint,
          s3_use_ssl = TRUE,
          s3_url_style = "path"
        )
        
        print("creating summaries")
        
        success <- tryCatch({
          s3_read$CreateDir(paste0("summaries"))
          fc |>
            dplyr::summarise(prediction = mean(prediction), .by = dplyr::any_of(c("site_id", "datetime", "reference_datetime", "family", "duration", "model_id",
                                                                                  "parameter", "pub_datetime", "reference_date", "variable", "project_id"))) |>
            score4cast::summarize_forecast(extra_groups = c("duration", "project_id")) |>
            dplyr::mutate(reference_date = lubridate::as_date(reference_datetime)) |>
            duckdbfs::write_dataset(paste0(config$processed_sub_bucket), format = 'parquet',
                                 partitioning = c("project_id",
                                                  "duration",
                                                  "variable",
                                                  "model_id",
                                                  "reference_date"),
                                   options = list("PER_THREAD_OUTPUT false"))
          TRUE  # success
        }, error = function(e) {
          cat("ERROR processing forecast:", conditionMessage(e), "\n")
          cat("Skipping entire iteration...\n\n")
          FALSE  # failure
        })
        
        # Skip to next file
        if (!success) next

        print('filling sumbission parquet')
        
        fc |> duckdbfs::write_dataset(paste0(config$sub_parquet_bucket),
                                      format = 'parquet',
                                      partitioning = c("project_id",
                                                       "duration",
                                                       "variable",
                                                       "model_id",
                                                       "reference_date"),
                                      options = list("PER_THREAD_OUTPUT false"))
        
        #submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        #fs::file_copy(submissions[i], submission_timestamp)
        raw_submissions_object <- file.path(config$raw_submissions_bucket, curr_submission)
        print('Raw Submissions Object:')
        print(raw_submissions_object)
        #raw_bucket_object <- paste0(config$raw_submissions_bucket, basename(submission_timestamp))

        
        minioclient::mc_cp(paste0(config$submissions_write_bucket, curr_submission),
                           raw_submissions_object)
        #minioclient::mc_cp(submission_timestamp, paste0(dirname(raw_bucket_object),"/", basename(submission_timestamp)))
        minioclient::mc_rm(paste0(config$submissions_write_bucket, curr_submission))
        
        print("finishing submission processing")

        rm(fc)
        gc()

      } else {

        submission_timestamp <- paste0(submission_dir,"/T", time_stamp, "_", basename(submissions[i]))
        fs::file_copy(submissions[i], submission_timestamp)
        raw_bucket_object <- paste0("s3_store/",config$forecasts_bucket,"/raw/",basename(submission_timestamp))

        minioclient::mc_cp(submission_timestamp, paste0(dirname(raw_bucket_object),"/", basename(submission_timestamp)))

        if(length(minioclient::mc_ls(raw_bucket_object)) > 0){
          minioclient::mc_rm(file.path("submit",config$submissions_bucket,curr_submission))
        }

      }
    }
  }

}

unlink(local_dir, recursive = TRUE)

message(paste0("Completed Processing Submissions ", Sys.time()))
