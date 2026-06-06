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
handlers(global = TRUE)
handlers("cli")

install_mc()
config <- read_yaml("challenge_configuration.yaml")
print('Read in config')

# Define bucket locations
# Not sure what these should be
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

#duckdb_secrets(endpoint = config$submissions_endpoint , key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = forecasts_bucket_base)
print('duckdb access works')

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

# x <- mc_ls("osn/bu4cast-ci-write/challenges/project_id=bu4cast/parquet/project_id=bu4cast/duration=P1D/variable=NO2_P1H/model_id=tg_dgam",
#            recursive = TRUE, details = TRUE)
# print(x)
# nrow(x)
# names(x)

library(arrow)
library(dplyr)
library(stringr)

bundle_me <- function(path) {
  
  print(paste("Processing:", path))
  
  # Extract bucket and path components
  path_clean <- sub("^s3://", "", path)
  bucket <- str_split(path_clean, "/")[[1]][1]
  prefix <- sub(paste0("^", bucket, "/"), "", path_clean)
  
  print(paste("Bucket:", bucket))
  print(paste("Prefix:", prefix))
  
  # Set up S3 for arrow
  key_id <- Sys.getenv("OSN_KEY", "")
  secret <- Sys.getenv("OSN_SECRET", "")
  
  tryCatch({
    # Create S3 filesystem directly
    s3_fs <- S3FileSystem$create(
      endpoint_override = "minio-s3.apps.shift.nerc.mghpcc.org",
      access_key = key_id,
      secret_key = secret,
      region = "us-east-1",
      scheme = "https"
    )
    
    print("S3 filesystem created successfully")
    
    # List files using FileSelector
    selector <- FileSelector$create(
      paste0(prefix, "/"),
      recursive = TRUE,
      file_type = "parquet"
    )
    
    files <- s3_fs$GetFileInfo(selector)
    parquet_files <- files[!files$type == "Directory", ]
    
    if(length(parquet_files) == 0) {
      stop("No parquet files found")
    }
    
    print(paste("Found", length(parquet_files), "parquet files"))
    
    # Create full S3 URIs
    file_paths <- paste0("s3://", bucket, "/", parquet_files$path)
    
    # Read all files into a single dataset
    ds <- open_dataset(
      file_paths,
      filesystem = s3_fs,
      partitioning = hive_partitioning(),
      format = "parquet"
    )
    
    # Filter
    filtered <- ds %>%
      filter(!is.na(model_id),
             !is.na(parameter),
             !is.na(prediction))
    
    print(paste("Filtered dataset has", nrow(filtered), "rows"))
    
    # Create bundled path
    bundled_prefix <- str_replace(prefix, fixed("/parquet"), "/bundled-parquet")
    
    # Check if bundled file already exists
    bundled_exists <- tryCatch({
      bundled_selector <- FileSelector$create(
        paste0(bundled_prefix, "/"),
        recursive = FALSE
      )
      length(s3_fs$GetFileInfo(bundled_selector)) > 0
    }, error = function(e) FALSE)
    
    # Read existing bundled data if it exists
    if(bundled_exists) {
      old_ds <- open_dataset(
        paste0("s3://", bucket, "/", bundled_prefix, "/"),
        filesystem = s3_fs,
        format = "parquet"
      )
      
      # Combine (remove duplicates if needed)
      filtered <- union_all(old_ds, filtered)
      print("Merged with existing bundled data")
    }
    
    # Write as single parquet file (no partitioning)
    output_path <- paste0("s3://", bucket, "/", bundled_prefix, "/bundled.parquet")
    
    write_dataset(
      filtered,
      output_path,
      filesystem = s3_fs,
      format = "parquet"
    )
    
    print(paste("Bundled data written to:", output_path))
    
    return(TRUE)
    
  }, error = function(e) {
    print(paste("Error:", e$message))
    stop("Failed to bundle data")
  })
}

bundle_me_minio <- function(path) {
  library(arrow)
  library(dplyr)
  library(stringr)
  
  print(paste("Processing:", path))
  
  # Set up MinIO S3 credentials
  key_id <- Sys.getenv("OSN_KEY", "")
  secret <- Sys.getenv("OSN_SECRET", "")
  
  # Create bundled path
  bundled_path <- str_replace(path, fixed("/parquet"), "/bundled-parquet")
  
  # Create S3FileSystem with MinIO configuration
  s3_fs <- S3FileSystem$create(
    endpoint_override = "https://minio-s3.apps.shift.nerc.mghpcc.org",
    access_key = key_id,
    secret_key = secret,
    region = "us-east-1",
    # MinIO often requires these additional settings
    scheme = "https",
    allow_bucket_creation = FALSE,
    allow_bucket_deletion = FALSE
  )
  
  print("S3FileSystem created for MinIO")
  
  # Extract bucket and key from path
  # s3://bucket/path/to/files
  path_parts <- str_split(sub("^s3://", "", path), "/")[[1]]
  bucket <- path_parts[1]
  key <- paste(path_parts[-1], collapse = "/")
  
  print(paste("Bucket:", bucket))
  print(paste("Key:", key))
  
  # List files using the S3FileSystem
  tryCatch({
    # Get file info from the S3 bucket
    files <- s3_fs$GetFileInfo(paste0(bucket, "/", key, "/"))
    print(paste("Found", length(files), "items in directory"))
    
    # Filter for files (not directories)
    file_paths <- files[files$type != "Directory", ]$path
    
    if(length(file_paths) == 0) {
      # Try to list recursively
      selector <- FileSelector$create(
        paste0(bucket, "/", key, "/"),
        recursive = TRUE
      )
      files <- s3_fs$GetFileInfo(selector)
      file_paths <- files[files$type != "Directory", ]$path
    }
    
    # Filter for parquet files
    parquet_files <- file_paths[grepl("\\.parquet$", file_paths)]
    
    if(length(parquet_files) == 0) {
      stop("No parquet files found")
    }
    
    print(paste("Found", length(parquet_files), "parquet files"))
    
    # Create full S3 URIs
    full_paths <- paste0("s3://", parquet_files)
    
    # Read the dataset
    ds <- open_dataset(
      full_paths,
      filesystem = s3_fs,
      partitioning = hive_partitioning(),
      format = "parquet"
    )
    
    # Filter
    filtered <- ds %>%
      filter(!is.na(model_id),
             !is.na(parameter),
             !is.na(prediction))
    
    # Write bundled data
    # Extract bucket and key for bundled path
    bundled_parts <- str_split(sub("^s3://", "", bundled_path), "/")[[1]]
    bundled_bucket <- bundled_parts[1]
    bundled_key <- paste(bundled_parts[-1], collapse = "/")
    
    write_dataset(
      filtered,
      paste0(bundled_bucket, "/", bundled_key, "/bundled.parquet"),
      filesystem = s3_fs,
      format = "parquet"
    )
    
    print(paste("Successfully wrote bundled data to:", bundled_path))
    
    return(TRUE)
    
  }, error = function(e) {
    print(paste("Error:", e$message))
    return(FALSE)
  })
}

library(dplyr)
library(stringr)

bundle_me_simple <- function(path) {
  
  print(paste("Processing:", path))
  
  # Convert S3 path to mc path
  mc_path <- str_replace(path, fixed("s3://"), "osn/")
  
  # Create bundled path
  bundled_path <- str_replace(path, fixed("/parquet"), "/bundled-parquet")
  mc_bundled_path <- str_replace(bundled_path, fixed("s3://"), "osn/")
  
  print(paste("MC Path:", mc_path))
  
  # List all parquet files
  all_items <- mc_ls(mc_path, recursive = TRUE, details = TRUE)
  
  # Filter for parquet files
  parquet_files <- all_items %>%
    filter(str_detect(name, "\\.parquet$")) %>%
    filter(!is_folder) %>%
    pull(path)
  
  if(length(parquet_files) == 0) {
    stop("No parquet files found")
  }
  
  print(paste("Found", length(parquet_files), "parquet files"))
  
  # Process files in batches to avoid memory issues
  batch_size <- 10
  all_data <- list()
  
  for(i in seq(1, length(parquet_files), batch_size)) {
    batch_end <- min(i + batch_size - 1, length(parquet_files))
    batch_files <- parquet_files[i:batch_end]
    
    print(paste("Processing batch", ceiling(i/batch_size), 
                "files", i, "to", batch_end))
    
    # Download and process batch
    batch_data <- lapply(batch_files, function(mc_file) {
      # Create temp file
      temp_file <- tempfile(fileext = ".parquet")
      
      # Download
      mc_cp(mc_file, temp_file)
      
      # Read with arrow
      tryCatch({
        df <- arrow::read_parquet(temp_file)
        
        # Filter
        df_filtered <- df %>%
          filter(!is.na(model_id),
                 !is.na(parameter),
                 !is.na(prediction))
        
        # Clean up
        unlink(temp_file)
        
        return(df_filtered)
      }, error = function(e) {
        print(paste("Error reading", mc_file, ":", e$message))
        unlink(temp_file)
        return(NULL)
      })
    })
    
    # Remove NULLs and combine
    batch_data <- batch_data[!sapply(batch_data, is.null)]
    if(length(batch_data) > 0) {
      all_data <- c(all_data, batch_data)
    }
  }
  
  if(length(all_data) == 0) {
    stop("No valid data found in any files")
  }
  
  # Combine all data
  combined <- bind_rows(all_data)
  
  print(paste("Combined data has", nrow(combined), "rows"))
  
  # Check for existing bundled data
  existing_files <- tryCatch({
    mc_ls(mc_bundled_path, details = TRUE) %>%
      filter(str_detect(name, "\\.parquet$")) %>%
      filter(!is_folder) %>%
      pull(path)
  }, error = function(e) {
    character()
  })
  
  # Read and merge existing data if any
  if(length(existing_files) > 0) {
    print(paste("Found", length(existing_files), "existing bundled files"))
    
    existing_data <- lapply(existing_files, function(mc_file) {
      temp_file <- tempfile(fileext = ".parquet")
      mc_cp(mc_file, temp_file)
      df <- arrow::read_parquet(temp_file)
      unlink(temp_file)
      return(df)
    })
    
    existing_combined <- bind_rows(existing_data)
    combined <- bind_rows(existing_combined, combined)
    
    print(paste("After merging with existing:", nrow(combined), "rows"))
  }
  
  # Create bundled directory if it doesn't exist
  if(!mc_dir_exists(mc_bundled_path)) {
    mc_mkdir(mc_bundled_path, recursive = TRUE)
  }
  
  # Write to local temp file
  temp_output <- tempfile(fileext = ".parquet")
  arrow::write_parquet(combined, temp_output)
  
  # Upload to S3
  mc_output_path <- paste0(mc_bundled_path, "bundled.parquet")
  mc_cp(temp_output, mc_output_path)
  
  print(paste("Uploaded bundled data to:", mc_output_path))
  
  # Clean up
  unlink(temp_output)
  
  return(TRUE)
}


bundle_me_old <- function(path, conn) {
  
  # Ensure httpfs is loaded
  tryCatch({
    DBI::dbExecute(conn, "LOAD httpfs;")
    print("dbExecute LOAD ran")
  }, error = function(e) {
    tryCatch({
      DBI::dbExecute(conn, "INSTALL httpfs;")
      DBI::dbExecute(conn, "LOAD httpfs;")
      print("dbExecute INSTALL and LOAD ran")
    }, error = function(e2) {
      print(paste("Could not load httpfs:", e2$message))
    })
  })

  # Get paths set up
  print(paste0("Path: ", path))
  #con = duckdbfs::cached_connection(tempfile())
  #duckdb_secrets(endpoint = config$endpoint, key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = forecasts_bucket_base)
  bundled_path <- path |> str_replace(fixed("/parquet"), "/bundled-parquet")
  print(paste0("Bundled Path: ", bundled_path))
  glob_path <- paste0(path, "**/*.parquet")
  print(paste0("Glob Path: ", glob_path))
  
  # Try the direct read_parquet approach first
  tryCatch({
    # Use simpler pattern - just directory listing
    sql_query <- sprintf(
      "CREATE OR REPLACE TABLE tmp_new_data AS 
      SELECT * 
      FROM read_parquet('%s*/*.parquet', HIVE_PARTITIONING=TRUE)
      WHERE model_id IS NOT NULL
      AND parameter IS NOT NULL
      AND prediction IS NOT NULL",
      path
    )
    
    print("Executing query...")
    DBI::dbExecute(conn, sql_query)
    
    # Write filtered data to local temp file
    DBI::dbExecute(conn, "COPY tmp_new_data TO 'tmp_new.parquet' (FORMAT PARQUET)")
    
    print('Created tmp_new.parquet')
    
  }, error = function(e) {
    print(paste("Error with read_parquet:", e$message))
    
    # Try alternative: Use explicit SQL with s3() function
    print("Trying s3() function...")
    
    # Extract bucket and path
    path_parts <- str_split(sub("^s3://", "", path), "/")[[1]]
    bucket <- path_parts[1]
    prefix <- paste(path_parts[-1], collapse = "/")
    
    # Use s3() with simpler pattern
    s3_pattern <- paste0("s3://", bucket, "/", prefix, "*/*.parquet")
    
    sql_query <- sprintf(
      "CREATE OR REPLACE TABLE tmp_new_data AS 
       SELECT * 
       FROM s3('%s', HIVE_PARTITIONING=TRUE)
       WHERE model_id IS NOT NULL
         AND parameter IS NOT NULL
         AND prediction IS NOT NULL",
      s3_pattern
    )
    
    DBI::dbExecute(conn, sql_query)
    DBI::dbExecute(conn, "COPY tmp_new_data TO 'tmp_new.parquet' (FORMAT PARQUET)")
    print('Created tmp_new.parquet using s3()')
  })
  
  # open_dataset(path, conn = con) |>
  #   filter( !is.na(model_id),
  #           !is.na(parameter),
  #           !is.na(prediction)) |>
  #   write_dataset("tmp_new.parquet")
  # 
  # print('created tmp_new.parquet')
  
  # special filters should not be needed on bundled copy
  # Only if model has bundled entries!
  
  old <- tryCatch({
    bundled_path_with_glob <- paste0(bundled_path, "*.parquet")
    DBI::dbExecute(con, paste0(
      "CREATE OR REPLACE TABLE tmp_old AS ",
      "SELECT * FROM read_parquet('", bundled_path_with_glob, "', HIVE_PARTITIONING=TRUE)"
    ))
    old <- open_dataset("tmp_old", conn = conn)
  },
  error = function(e) NULL
  )
  
  # old <- tryCatch({
  # open_dataset(bundled_path, conn = con) |>
  #    write_dataset("tmp_old.parquet")
  # old <- open_dataset("tmp_old.parquet")
  # },
  # # no new data
  # error = function(e) NULL
  # )

  print('checked for old parquet')
  
  # these are both local, so we can stream back.
  new <- open_dataset("tmp_new.parquet")

## We can just "append", we no longer face duplicates:
# by <- join_by(datetime, site_id, prediction, parameter, family, reference_datetime, pub_datetime, duration, model_id, project_id, variable)
#  filtered_n <- old |> anti_join(new, by = by) |> count() |> pull(n) # is this the bottleneck?
#  previous_n <- open_dataset("tmp_old.parquet") |> count() |> pull(n)
#  stopifnot(previous_n - filtered_n == 0)

  ## no partition levels left so we must write to an explicit .parquet
  if(!is.null(old)) {
    bundled_dir <- bundled_path |> str_replace(fixed("s3://"), "osn/") |> mc_ls(details = TRUE)
    mc_bundled_path <- bundled_dir |> filter(!is_folder) |> pull(path)
    stopifnot(length(mc_bundled_path) == 1)
    bundled_path <- mc_bundled_path |> str_replace(fixed("osn/"), fixed("s3://"))

    new <- union(old, new)
  }
  
  print('merged new and old')
  
  ## once running consistently we can "append" with union_all instead of union
  # uses less RAM. since mc_rm / mc_mv removes anything we have already read
  new |>
    write_dataset(bundled_path,
                  options = list("PER_THREAD_OUTPUT false"))

  print('write merged to bundled path')
  
  #We should now archive anything we have bundled:
  mc_path <- path |> str_replace(fixed("s3://"), "osn/")
  dest_path <- mc_path |>
    str_replace(fixed("/parquet"), "/archive-parquet")
  mc_mv(mc_path, dest_path, recursive = TRUE)

  print('archive')
  
  # clears up empty folders (not necessary?)
  mc_rm(mc_path, recursive = TRUE)
  
  print('empty folders')

  #duckdbfs::close_connection(con); gc()

  invisible(path)
}




# We use future_apply framework to show progress while being robust to OOM kils.
# We are not actually running on multi-core, which would be RAM-inefficient
future::plan(future::sequential)

safe_bundles <- function(xs) {
  p <- progressor(along = xs)
  future_lapply(xs, function(x, ...) {
    out <- bundle_me_simple(x)
    p(sprintf("x=%s", x))
    out
  },  future.seed = TRUE)
}


bench::bench_time({
  out <- safe_bundles(model_paths)
})
# print(out)



# bundled count at end
count <- open_dataset(paste0("s3://", forecast_bundled_parquet_bucket),
                      s3_endpoint = config$endpoint,
                      anonymous = TRUE) |>
  count()
print(count)



most_recent <- open_dataset(paste0("s3://", forecast_bundled_parquet_bucket),
             s3_endpoint = config$endpoint,
             anonymous = TRUE) |>
  group_by(model_id, variable) |>
  summarise(most_recent = max(reference_datetime)) |>
  arrange(desc(most_recent))
print(most_recent)



# should we slice_max(pub_time) to ensure only most recent pub_time if duplicates submitted?
# grouping <- c("model_id", "reference_datetime", "site_id", "datetime", "family", "variable", "duration", "project_id")
