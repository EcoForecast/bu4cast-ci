remotes::install_github("cboettig/duckdbfs", upgrade=FALSE)

library(tidyverse)
library(duckdbfs)
library(duckdb)
library(DBI)
library(minioclient)
library(bench)
library(glue)
library(fs)
library(future.apply)
library(progressr)
library(yaml)
library(arrow)
library(dplyr)
library(stringr)
handlers(global = TRUE)
handlers("cli")

install_mc()
config <- read_yaml("challenge_configuration.yaml")
print('Read in config')

# Define bucket locations
# Not sure what these should be (I think these are right but I would double check)
forecast_parquet_bucket <- sub("^s3://", "", config$sub_parquet_bucket)
forecast_bundled_parquet_bucket <- paste0(config$s3_bucket_write, "/challenges/project_id=", config$project_id, "/bundled-parquet/")
forecasts_bucket_base <- paste0(config$s3_bucket_write, '/', config$submissions_bucket)
print(forecast_parquet_bucket)
print(forecast_bundled_parquet_bucket)
print(forecasts_bucket_base)

# Prep Minio Access
minioclient::mc_alias_set("osn",
                          config$submissions_endpoint,
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))
# mc_alias_set("nrp", "s3-west.nrp-nautilus.io", Sys.getenv("EFI_NRP_KEY"), Sys.getenv("EFI_NRP_SECRET"))
print('mc access works')

# Connect to DuckDB (this is commented out atm bc I want to do it later)
# key_id   <- Sys.getenv("OSN_KEY", "")
# secret   <- Sys.getenv("OSN_SECRET", "")

# conn <- dbConnect(duckdb())
# DBI::dbExecute(conn, "INSTALL httpfs;")
# DBI::dbExecute(conn, "LOAD httpfs;")

# sql <- sprintf("
#   CREATE OR REPLACE SECRET s3_minio_osn (
#     TYPE S3,
#     KEY_ID '%s',
#     SECRET '%s',
#     ENDPOINT 'https://minio-s3.apps.shift.nerc.mghpcc.org',
#     REGION 'us-east-1',
#     USE_SSL = true,
#     URL_STYLE = 'path'
#   )
# ", key_id, secret)

# DBI::dbExecute(conn, sql)

#duckdb_secrets(endpoint = config$submissions_endpoint , key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = forecasts_bucket_base)
#print('duckdb access works')

remote_path <- paste0("osn/", forecast_parquet_bucket)
contents <- mc_ls(remote_path, recursive = TRUE, details = TRUE)
data_paths <- contents |> filter(!is_folder) |> pull(path)

# model paths are paths with at least one reference_datetime containing data files
model_paths <-
  data_paths |>
  str_replace_all("reference_date=\\d{4}-\\d{2}-\\d{2}/.*", "") |>
  str_replace("^osn\\/", "s3://") |>
  unique()

print(model_paths)

# bundled count at start
# count <- open_dataset(paste0("s3://", forecast_bundled_parquet_bucket),
#                       s3_endpoint = config$endpoint,
#                       anonymous = FALSE) |>
#   count()
# print(count)
bundled_remote_path <- paste0("osn/", forecast_bundled_parquet_bucket)
bundled_contents <- mc_ls(bundled_remote_path, recursive = TRUE, details = TRUE)
count <- if (nrow(bundled_contents) == 0) 0 else sum(!bundled_contents$is_folder)
print(count)

# This is testing to see if we can read from the folder
# x <- mc_ls("osn/bu4cast-ci-write/challenges/project_id=bu4cast/parquet/project_id=bu4cast/duration=P1D/variable=NO2_P1H/model_id=tg_dgam",
#            recursive = TRUE, details = TRUE)
# print(x)
# nrow(x)
# names(x)

library(duckdb)

con <- dbConnect(duckdb())
DBI::dbExecute(con, "INSTALL httpfs;")
DBI::dbExecute(con, "LOAD httpfs;")

# Create DuckDB connection
dbExecute(con, sprintf("
  CREATE SECRET (
    TYPE S3,
    KEY_ID '%s',
    SECRET '%s',
    ENDPOINT 'minio-s3.apps.shift.nerc.mghpcc.org',
    USE_SSL true,
    URL_STYLE 'path'
  )
", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET")))

# Loop through each model path
for (path in model_paths) {

  # Prep paths
  print(paste("Processing:", path))
  bundled_path <- path |> str_replace(fixed("/parquet"), "/bundled-parquet")
  print(paste("Bundled Path:", bundled_path))
  
  # Build S3 query paths
  s3_query_path <- paste0(path, "**/*.parquet")
  s3_query_bundled_path <- paste0(bundled_path, "**/*.parquet")

  # Load all new data from parquet
  tryCatch({
    # Read all parquet files with hive partitioning
    query <- sprintf("
      SELECT *
      FROM read_parquet('%s', hive_partitioning = true)
      WHERE model_id IS NOT NULL
      AND parameter IS NOT NULL
      AND prediction IS NOT NULL
    ", s3_query_path)
    
    tmp_new <- dbGetQuery(con, query)
    print(paste("Read", nrow(tmp_new), "rows"))
    print(paste("Columns:", paste(colnames(tmp_new), collapse = ", ")))
    
  }, error = function(e) {
    print(paste("Error reading new path ", path, ":", e$message))
  })

  # Load all old data from parquet
  tmp_old <- tryCatch({
    # Read all parquet files with hive partitioning
    query_old <- sprintf("
      SELECT *
      FROM read_parquet('%s', hive_partitioning = true)
    ", s3_query_bundled_path)
    
    tmp_old <- dbGetQuery(con, query_old)
    print(paste("Read", nrow(tmp_old), "rows"))
    print(paste("Columns:", paste(colnames(tmp_old), collapse = ", ")))

    # Return data
    tmp_old
    
  },
  error = function(e) {
    print(paste("No new data for ", s3_query_bundled_path))
    NULL
  })
  
  # Combine if old data was found
  if(!is.null(old)) {
    bundled_dir <- bundled_path |> str_replace(fixed("s3://"), "osn/") |> mc_ls(details = TRUE)
    mc_bundled_path <- bundled_dir |> filter(!is_folder) |> pull(path)
    stopifnot(length(mc_bundled_path) == 1)
    bundled_path <- mc_bundled_path |> str_replace(fixed("osn/"), fixed("s3://"))
  
    new <- union(tmp_old, tmp_new)
  } else {
    new <- tmp_new
  }

  # Write data to bundled
  new |> write_dataset(bundled_path, options = list("PER_THREAD_OUTPUT false"))

  # Create archived parquet
  mc_path <- path |> str_replace(fixed("s3://"), "osn/")
  dest_path <- mc_path |> str_replace(fixed("/parquet"), "/archive-parquet")
  mc_mv(mc_path, dest_path, recursive = TRUE)

  # clears up empty folders (not necessary?)
  mc_rm(mc_path, recursive = TRUE)
}

print("For Loop Complete")
                     
# Bundled count at end
bundled_remote_path <- paste0("osn/", forecast_bundled_parquet_bucket)
bundled_contents <- mc_ls(bundled_remote_path, recursive = TRUE, details = TRUE)
count <- if (nrow(bundled_contents) == 0) 0 else sum(!bundled_contents$is_folder)
print(count)

# This is a function from the previous iteration

bundle_me <- function(path) {
  
  print(paste("Processing:", path))
  
  # Create a new connection for this function call
  con = duckdbfs::cached_connection(tempfile())
  
  # Use the same S3 credentials setup
  on.exit({
    duckdbfs::close_connection(con)
    gc()
  })
  
  # Create bundled path: replace /parquet/ with /bundled-parquet/
  bundled_path <- path |> 
    str_replace(fixed("/parquet/"), "/bundled-parquet/")
  
  print(paste("Bundled path:", bundled_path))
  
  # Use DuckDB to read and filter the data
  # Create a temporary view from the partitioned parquet data
  tryCatch({
    # Use DuckDB's read_parquet with glob pattern
    sql_query <- sprintf(
      "CREATE OR REPLACE TABLE tmp_new_data AS 
       SELECT * 
       FROM read_parquet('%s/**/*.parquet', HIVE_PARTITIONING=TRUE)
       WHERE model_id IS NOT NULL
         AND parameter IS NOT NULL
         AND prediction IS NOT NULL",
      path
    )
    
    DBI::dbExecute(con, sql_query)
    
    # Write filtered data to local temporary parquet file
    DBI::dbExecute(con, "COPY tmp_new_data TO 'tmp_new.parquet' (FORMAT PARQUET)")
    
    print('Created tmp_new.parquet')
    
  }, error = function(e) {
    # Fallback: Use old approach
    print(paste("Error with DuckDB approach:", e$message))
    print("Trying arrow/dplyr approach...")
    
    open_dataset(path, partitioning = hive_partitioning()) |>
      filter(!is.na(model_id),
             !is.na(parameter),
             !is.na(prediction)) |>
      write_dataset("tmp_new.parquet")
  })
  
  # Check for existing bundled data
  old <- tryCatch({
    # Try to read existing bundled data
    sql_query_old <- sprintf(
      "CREATE OR REPLACE TABLE tmp_old AS 
       SELECT * FROM read_parquet('%s*.parquet')",
      bundled_path
    )
    
    DBI::dbExecute(con, sql_query_old)
    
    # Write old data to local file for arrow to read
    DBI::dbExecute(con, "COPY tmp_old TO 'tmp_old.parquet' (FORMAT PARQUET)")
    
    open_dataset("tmp_old.parquet")
  }, error = function(e) {
    print(paste("No existing bundled data:", e$message))
    NULL
  })
  
  # Load new data
  new <- open_dataset("tmp_new.parquet")
  
  # Merge if old exists
  if(!is.null(old)) {
    new <- union(old, new)
    print("Merged with existing bundled data")
  }
  
  # Write merged data to bundled path
  new |>
    write_dataset(bundled_path,
                  options = list("PER_THREAD_OUTPUT" = FALSE))
  
  print(paste('Wrote merged data to:', bundled_path))
  
  # Archive original data
  mc_path <- path |> str_replace(fixed("s3://"), "osn/")
  dest_path <- mc_path |>
    str_replace(fixed("/parquet/"), "/archive-parquet/")
  
  if(mc_dir_exists(mc_path)) {
    mc_mv(mc_path, dest_path, recursive = TRUE)
    print('Archived original data')
  }
  
  # Clean up temporary files
  if(file.exists("tmp_new.parquet")) unlink("tmp_new.parquet")
  if(file.exists("tmp_old.parquet")) unlink("tmp_old.parquet")
  
  invisible(path)
}

# This is what was in the original file and is how the bundle me function runs

# # Run it
# print("Running result")
# result <- bundle_me_simple_working(model_paths[1])
# print("Result ran")

# We use future_apply framework to show progress while being robust to OOM kils.
# We are not actually running on multi-core, which would be RAM-inefficient
future::plan(future::sequential)

# safe_bundles <- function(xs) {
#   p <- progressor(along = xs)
#   future_lapply(xs, function(x, ...) {
#     out <- test_bundle_debug(x)
#     p(sprintf("x=%s", x))
#     out
#   },  future.seed = TRUE)
# }


# bench::bench_time({
#   out <- safe_bundles(model_paths)
# })
# # print(out)



# # bundled count at end
# count <- open_dataset(paste0("s3://", forecast_bundled_parquet_bucket),
#                       s3_endpoint = config$endpoint,
#                       anonymous = TRUE) |>
#   count()
# print(count)



# most_recent <- open_dataset(paste0("s3://", forecast_bundled_parquet_bucket),
#              s3_endpoint = config$endpoint,
#              anonymous = TRUE) |>
#   group_by(model_id, variable) |>
#   summarise(most_recent = max(reference_datetime)) |>
#   arrange(desc(most_recent))
# print(most_recent)



# should we slice_max(pub_time) to ensure only most recent pub_time if duplicates submitted?
# grouping <- c("model_id", "reference_datetime", "site_id", "datetime", "family", "variable", "duration", "project_id")

