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

bundle_me_simple <- function(path) {
  
  print(paste("Processing:", path))
  
  # Convert S3 path to mc path
  mc_path <- str_replace(path, fixed("s3://"), "osn/")
  
  # Create bundled path
  bundled_path <- str_replace(path, fixed("/parquet"), "/bundled-parquet")
  mc_bundled_path <- str_replace(bundled_path, fixed("s3://"), "osn/")
  
  print(paste("MC Path:", mc_path))
  
  # List all items - check what mc_ls returns
  all_items <- mc_ls(mc_path, recursive = TRUE)
  
  print(paste("mc_ls returned", ifelse(is.data.frame(all_items), 
                                       paste(nrow(all_items), "rows"),
                                       "not a dataframe")))
  
  # Debug: print column names
  if(is.data.frame(all_items)) {
    print(paste("Columns:", paste(names(all_items), collapse = ", ")))
    print("First few rows:")
    print(head(all_items))
  }
  
  # Filter for parquet files - adjust based on actual column names
  if(is.data.frame(all_items)) {
    # Try to find the right column name
    name_col <- NA
    if("name" %in% names(all_items)) {
      name_col <- "name"
    } else if("key" %in% names(all_items)) {
      name_col <- "key"
    } else if("filename" %in% names(all_items)) {
      name_col <- "filename"
    } else if(length(names(all_items)) > 0) {
      # Use first column
      name_col <- names(all_items)[1]
    }
    
    if(!is.na(name_col)) {
      print(paste("Using column", name_col, "for filtering"))
      
      # Filter for parquet files
      parquet_files <- all_items %>%
        filter(grepl("\\.parquet$", .data[[name_col]])) %>%
        pull(path)
      
      print(paste("Found", length(parquet_files), "parquet files"))
      
      if(length(parquet_files) == 0) {
        stop("No parquet files found")
      }
      
    } else {
      stop("Could not find appropriate column in mc_ls output")
    }
  } else {
    # mc_ls might return a vector
    if(is.character(all_items)) {
      parquet_files <- all_items[grepl("\\.parquet$", all_items)]
      print(paste("Found", length(parquet_files), "parquet files"))
      
      if(length(parquet_files) == 0) {
        stop("No parquet files found")
      }
    } else {
      stop("mc_ls returned unexpected format")
    }
  }
  
  # Process files in batches
  batch_size <- 10
  all_data <- list()
  
  for(i in seq(1, length(parquet_files), batch_size)) {
    batch_end <- min(i + batch_size - 1, length(parquet_files))
    batch_files <- parquet_files[i:batch_end]
    
    print(paste("Processing batch", ceiling(i/batch_size), 
                "files", i, "to", batch_end))
    
    batch_data <- lapply(batch_files, function(mc_file) {
      # Create temp file
      temp_file <- tempfile(fileext = ".parquet")
      
      # Download
      tryCatch({
        mc_cp(mc_file, temp_file)
      }, error = function(e) {
        print(paste("Failed to download", mc_file, ":", e$message))
        return(NULL)
      })
      
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
    existing_items <- mc_ls(mc_bundled_path, recursive = FALSE)
    
    if(is.data.frame(existing_items)) {
      # Filter for parquet files
      name_col <- NA
      if("name" %in% names(existing_items)) {
        name_col <- "name"
      } else if("key" %in% names(existing_items)) {
        name_col <- "key"
      } else if(length(names(existing_items)) > 0) {
        name_col <- names(existing_items)[1]
      }
      
      if(!is.na(name_col)) {
        existing_items %>%
          filter(grepl("\\.parquet$", .data[[name_col]])) %>%
          pull(path)
      } else {
        character()
      }
    } else if(is.character(existing_items)) {
      existing_items[grepl("\\.parquet$", existing_items)]
    } else {
      character()
    }
  }, error = function(e) {
    character()
  })
  
  # Read and merge existing data if any
  if(length(existing_files) > 0) {
    print(paste("Found", length(existing_files), "existing bundled files"))
    
    existing_data <- lapply(existing_files, function(mc_file) {
      temp_file <- tempfile(fileext = ".parquet")
      tryCatch({
        mc_cp(mc_file, temp_file)
        df <- arrow::read_parquet(temp_file)
        unlink(temp_file)
        return(df)
      }, error = function(e) {
        print(paste("Error reading existing file", mc_file, ":", e$message))
        unlink(temp_file)
        return(NULL)
      })
    })
    
    # Remove NULLs
    existing_data <- existing_data[!sapply(existing_data, is.null)]
    
    if(length(existing_data) > 0) {
      existing_combined <- bind_rows(existing_data)
      combined <- bind_rows(existing_combined, combined)
      print(paste("After merging with existing:", nrow(combined), "rows"))
    }
  }
  
  # Create bundled directory if it doesn't exist
  if(!mc_dir_exists(mc_bundled_path)) {
    mc_mkdir(mc_bundled_path, recursive = TRUE)
  }
  
  # Write to local temp file
  temp_output <- tempfile(fileext = ".parquet")
  arrow::write_parquet(combined, temp_output)
  
  # Output file size
  output_size <- file.size(temp_output)
  print(paste("Local bundled file size:", output_size, "bytes"))
  
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

bundle_me_robust <- function(path) {
  
  print(paste("Processing:", path))
  
  # Convert S3 path to mc path
  mc_path <- str_replace(path, fixed("s3://"), "osn/")
  
  # Create bundled path
  bundled_path <- str_replace(path, fixed("/parquet"), "/bundled-parquet")
  mc_bundled_path <- str_replace(bundled_path, fixed("s3://"), "osn/")
  
  print(paste("MC Path:", mc_path))
  
  # List all items - simplest approach
  all_items <- mc_ls(mc_path, recursive = TRUE)
  
  # Debug
  print(paste("Type of all_items:", typeof(all_items)))
  
  # Handle different return types from mc_ls
  if(is.data.frame(all_items)) {
    # It's a dataframe
    parquet_files <- all_items$path[grepl("\\.parquet$", all_items$path)]
    
    # If no 'path' column, try other columns
    if(is.null(all_items$path) && length(all_items) > 0) {
      # Check all columns for path-like data
      for(col_name in names(all_items)) {
        if(any(grepl("\\.parquet$", all_items[[col_name]]))) {
          parquet_files <- all_items[[col_name]][grepl("\\.parquet$", all_items[[col_name]])]
          print(paste("Using column", col_name, "for paths"))
          break
        }
      }
    }
  } else if(is.character(all_items)) {
    # It's a character vector
    parquet_files <- all_items[grepl("\\.parquet$", all_items)]
  } else if(is.list(all_items)) {
    # It's a list, try to extract paths
    parquet_files <- unlist(all_items)[grepl("\\.parquet$", unlist(all_items))]
  } else {
    stop("mc_ls returned unexpected format")
  }
  
  if(length(parquet_files) == 0) {
    stop("No parquet files found")
  }
  
  print(paste("Found", length(parquet_files), "parquet files"))
  
  # Process first few files to test
  if(length(parquet_files) > 5) {
    test_files <- parquet_files[1:5]
  } else {
    test_files <- parquet_files
  }
  
  for(test_file in test_files) {
    print(paste("Sample file:", test_file))
  }
  
  # Process all files
  all_data <- list()
  
  for(i in seq_along(parquet_files)) {
    mc_file <- parquet_files[i]
    
    if(i %% 10 == 0) {
      print(paste("Processing file", i, "of", length(parquet_files)))
    }
    
    tryCatch({
      # Download
      temp_file <- tempfile(fileext = ".parquet")
      mc_cp(mc_file, temp_file)
      
      # Read
      df <- arrow::read_parquet(temp_file)
      
      # Filter
      df_filtered <- df %>%
        filter(!is.na(model_id),
               !is.na(parameter),
               !is.na(prediction))
      
      all_data[[i]] <- df_filtered
      
      # Clean up
      unlink(temp_file)
      
    }, error = function(e) {
      print(paste("Error processing", mc_file, ":", e$message))
    })
  }
  
  # Remove NULLs
  all_data <- all_data[!sapply(all_data, is.null)]
  
  if(length(all_data) == 0) {
    stop("No valid data found in any files")
  }
  
  # Combine
  combined <- bind_rows(all_data)
  print(paste("Total rows:", nrow(combined)))
  
  # Check for existing bundled data
  existing_exists <- tryCatch({
    mc_ls(mc_bundled_path)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  if(existing_exists) {
    tryCatch({
      existing_items <- mc_ls(mc_bundled_path, recursive = FALSE)
      existing_files <- unlist(existing_items)[grepl("\\.parquet$", unlist(existing_items))]
      
      if(length(existing_files) > 0) {
        print(paste("Found", length(existing_files), "existing bundled files"))
        
        # Download and read existing
        existing_data <- list()
        for(existing_file in existing_files) {
          temp_file <- tempfile(fileext = ".parquet")
          mc_cp(existing_file, temp_file)
          df <- arrow::read_parquet(temp_file)
          existing_data[[length(existing_data) + 1]] <- df
          unlink(temp_file)
        }
        
        existing_combined <- bind_rows(existing_data)
        combined <- bind_rows(existing_combined, combined)
        print(paste("After merge:", nrow(combined), "rows"))
      }
    }, error = function(e) {
      print(paste("Error reading existing:", e$message))
    })
  }
  
  # Ensure directory exists
  if(!mc_dir_exists(mc_bundled_path)) {
    mc_mkdir(mc_bundled_path, recursive = TRUE)
  }
  
  # Write and upload
  temp_output <- tempfile(fileext = ".parquet")
  arrow::write_parquet(combined, temp_output)
  
  mc_output <- paste0(mc_bundled_path, "bundled.parquet")
  mc_cp(temp_output, mc_output)
  
  print(paste("Created:", mc_output, 
              "Size:", file.size(temp_output), "bytes"))
  
  unlink(temp_output)
  
  return(TRUE)
}

# Debug version to understand the paths
test_bundle_debug <- function(path) {
  
  print(paste("Original path:", path))
  
  # Convert S3 path to mc path
  mc_path <- str_replace(path, fixed("s3://"), "osn/")
  
  print(paste("MC Path:", mc_path))
  
  # List with and without trailing slash
  mc_path_with_slash <- paste0(mc_path, "/")
  
  print("Testing mc_ls...")
  
  # Test 1: Without trailing slash
  result1 <- mc_ls(mc_path, recursive = TRUE)
  print(paste("Result 1 (no trailing slash):", length(result1), "items"))
  if(length(result1) > 0) {
    print("First 3 items:")
    print(head(result1, 3))
  }
  
  # Test 2: With trailing slash
  result2 <- mc_ls(mc_path_with_slash, recursive = TRUE)
  print(paste("Result 2 (with trailing slash):", length(result2), "items"))
  if(length(result2) > 0) {
    print("First 3 items:")
    print(head(result2, 3))
  }
  
  # Test 3: Non-recursive
  result3 <- mc_ls(mc_path_with_slash, recursive = FALSE)
  print(paste("Result 3 (non-recursive):", length(result3), "items"))
  if(length(result3) > 0) {
    print("All items:")
    print(result3)
  }
  
  # Now test downloading one file
  if(length(result1) > 0) {
    # Take first parquet file
    parquet_files <- result1[grepl("\\.parquet$", result1)]
    
    if(length(parquet_files) > 0) {
      test_file <- parquet_files[1]
      print(paste("Testing download of:", test_file))
      
      # Construct full path
      full_test_path <- paste0(mc_path, "/", test_file)
      print(paste("Full path:", full_test_path))
      
      temp_file <- tempfile(fileext = ".parquet")
      
      tryCatch({
        mc_cp(full_test_path, temp_file)
        print(paste("Download successful to:", temp_file))
        
        # Try to read
        df <- arrow::read_parquet(temp_file)
        print(paste("File read successfully. Columns:", 
                    paste(names(df), collapse = ", ")))
        print(paste("Rows:", nrow(df)))
        
        unlink(temp_file)
        
      }, error = function(e) {
        print(paste("Error:", e$message))
        unlink(temp_file)
      })
    }
  }
}

# SIMPLE WORKING SOLUTION
bundle_me_simple_working <- function(path) {
  
  print(paste("Processing:", path))
  
  # 1. Convert to mc path
  mc_path <- str_replace(path, fixed("s3://"), "osn/")
  
  # 2. Ensure it ends with /
  if(!str_ends(mc_path, "/")) {
    mc_path <- paste0(mc_path, "/")
  }
  
  # 3. Create bundled path
  mc_bundled_path <- str_replace(mc_path, fixed("/parquet/"), "/bundled-parquet/")
  
  print(paste("Source MC path:", mc_path))
  print(paste("Bundled MC path:", mc_bundled_path))
  
  # 4. List files using mc_ls - it returns character vector of relative paths
  all_files <- mc_ls(mc_path, recursive = TRUE)
  
  if(length(all_files) == 0) {
    stop("No files found in directory")
  }
  
  # 5. Filter for parquet files
  parquet_files <- all_files[grepl("\\.parquet$", all_files)]
  
  if(length(parquet_files) == 0) {
    stop("No parquet files found")
  }
  
  print(paste("Found", length(parquet_files), "parquet files"))
  
  # 6. IMPORTANT: mc_ls returns RELATIVE paths like "reference_date=2025-07-01/data_0.parquet"
  #    We need to prepend the source directory path
  full_parquet_paths <- paste0(mc_path, parquet_files)
  
  # 7. Process files
  all_data <- list()
  
  for(i in seq_along(full_parquet_paths)) {
    mc_file <- full_parquet_paths[i]
    
    if(i <= 3) {
      print(paste("Sample file path:", mc_file))
    }
    
    tryCatch({
      # Download to temp file
      temp_file <- tempfile(fileext = ".parquet")
      mc_cp(mc_file, temp_file)
      
      # Read with arrow
      df <- arrow::read_parquet(temp_file)
      
      # Filter for required columns
      if(all(c("model_id", "parameter", "prediction") %in% names(df))) {
        df_filtered <- df %>%
          filter(!is.na(model_id),
                 !is.na(parameter),
                 !is.na(prediction))
        
        if(nrow(df_filtered) > 0) {
          all_data[[length(all_data) + 1]] <- df_filtered
        }
      }
      
      unlink(temp_file)
      
    }, error = function(e) {
      print(paste("Error processing file:", e$message))
    })
  }
  
  if(length(all_data) == 0) {
    stop("No valid data found after filtering")
  }
  
  # 8. Combine all data
  combined <- bind_rows(all_data)
  print(paste("Combined data rows:", nrow(combined)))
  
  # 9. Check for existing bundled data
  tryCatch({
    existing_files <- mc_ls(mc_bundled_path, recursive = FALSE)
    existing_parquet <- existing_files[grepl("\\.parquet$", existing_files)]
    
    if(length(existing_parquet) > 0) {
      print(paste("Found", length(existing_parquet), "existing bundled files"))
      
      existing_data <- list()
      for(existing_file in existing_parquet) {
        tryCatch({
          full_existing_path <- paste0(mc_bundled_path, existing_file)
          temp_file <- tempfile(fileext = ".parquet")
          
          mc_cp(full_existing_path, temp_file)
          df <- arrow::read_parquet(temp_file)
          
          if(all(c("model_id", "parameter", "prediction") %in% names(df))) {
            existing_data[[length(existing_data) + 1]] <- df
          }
          
          unlink(temp_file)
        }, error = function(e) {
          print(paste("Error reading existing:", e$message))
        })
      }
      
      if(length(existing_data) > 0) {
        existing_combined <- bind_rows(existing_data)
        combined <- bind_rows(existing_combined, combined)
        print(paste("After merging with existing:", nrow(combined), "rows"))
      }
    }
  }, error = function(e) {
    print("No existing bundled data found")
  })
  
  # 10. Create bundled directory if needed
  if(!mc_dir_exists(mc_bundled_path)) {
    mc_mkdir(mc_bundled_path, recursive = TRUE)
    print(paste("Created directory:", mc_bundled_path))
  }
  
  # 11. Write bundled data
  temp_output <- tempfile(fileext = ".parquet")
  arrow::write_parquet(combined, temp_output)
  
  # Upload to bundled location
  mc_output <- paste0(mc_bundled_path, "bundled.parquet")
  mc_cp(temp_output, mc_output)
  
  print(paste("Successfully created bundled file:", mc_output))
  print(paste("File size:", file.size(temp_output), "bytes"))
  
  # Clean up
  unlink(temp_output)
  
  return(TRUE)
}

# Run it
print("Running result")
result <- bundle_me_simple_working(model_paths[1])
print("Result ran")

# We use future_apply framework to show progress while being robust to OOM kils.
# We are not actually running on multi-core, which would be RAM-inefficient
future::plan(future::sequential)

safe_bundles <- function(xs) {
  p <- progressor(along = xs)
  future_lapply(xs, function(x, ...) {
    out <- test_bundle_debug(x)
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
