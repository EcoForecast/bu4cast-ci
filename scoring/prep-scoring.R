#devtools::install_version("duckdb", "1.2.2")
remotes::install_github('cboettig/duckdbfs', upgrade = 'never')

library(dplyr)
library(duckdbfs)
library(progress)
library(bench)
library(yaml)
library(stringr)
library(minioclient)
library(DBI)

con <- duckdbfs::cached_connection(tempfile())
# ubuntu-latest GH-hosted runners have 4 vCPUs / 16GB RAM -- 64 threads here
# was a large oversubscription (each thread reserves its own working buffers),
# which was almost certainly OOM-killing the runner mid-write on the
# forecasts/scores/targets write_dataset() calls (exit 141 / broken pipe,
# with no output at all from inside that step -- consistent with a kill
# mid-operation rather than a query error). memory_limit caps usage so
# DuckDB spills to disk instead of exceeding the runner's RAM.
DBI::dbExecute(con, "SET THREADS=4;")
DBI::dbExecute(con, "SET memory_limit='12GB';")

setup_s3 <- function(con) {
  DBI::dbExecute(con, paste0("SET s3_access_key_id='", Sys.getenv("OSN_KEY"), "';"))
  DBI::dbExecute(con, paste0("SET s3_secret_access_key='", Sys.getenv("OSN_SECRET"), "';"))
  DBI::dbExecute(con, "SET s3_endpoint='uri.osn.mghpcc.org';")
  DBI::dbExecute(con, "SET s3_url_style='path';")
  DBI::dbExecute(con, "SET s3_use_ssl=true;")
  invisible(con)
}
setup_s3(con)

install_mc()
mc_alias_set("minio", "uri.osn.mghpcc.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))

remove_dir <- function(path) {
  tryCatch(
    {
      minioclient::mc_rm(path, recursive = TRUE)
      message('directory successfully removed...')
    },
    error = function(cond) {
      message("The removal directory could not be found...")
      message(conditionMessage(cond))
      NA
    },
    warning = function(cond) {
      message('Deleting the directory caused a warning...')
      message(conditionMessage(cond))
      NULL
    },
    finally = {
      message("Finished the delete portion...")
    }
  )
}

remove_dir("minio/bu4cast-ci/write/challenges/project_id=bu4cast/tmp/score_me")
remove_dir("minio/bu4cast-ci/write/challenges/project_id=bu4cast/tmp/forecasts")
remove_dir("minio/bu4cast-ci/write/challenges/project_id=bu4cast/tmp/targets")
remove_dir("minio/bu4cast-ci/write/challenges/project_id=bu4cast/tmp/scores")

config <- read_yaml("challenge_configuration.yaml")

forecast_bundled_parquet_bucket <- paste0(config$s3_bucket_write, "/write/challenges/project_id=", config$project_id, "/bundled-parquet/")
scores_bundled_parquet_bucket   <- paste0(config$scores_bucket, "/bundled-parquet/")

project        <- config$project_id
cut_off_date   <- Sys.Date() - lubridate::dmonths(6)
rescore        <- FALSE
obs_key_cols   <- c("project_id", "site_id", "datetime", "duration", "variable")
score_key_cols <- c(obs_key_cols, "model_id", "family", "reference_datetime")

# Build target file list, skipping any that don't exist in S3
# Check existence and build target file list
target_files <- NULL
for (i in 1:length(config$target_groups)) {
  grp      <- config$target_groups[[i]]
  filepath <- if (!is.null(grp$targets_corrected_filepath)) grp$targets_corrected_filepath else grp$targets_filepath
  path     <- paste0("s3://", config$s3_bucket_read, "/", filepath)
  exists <- tryCatch({
    DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) FROM read_csv_auto('%s', SAMPLE_SIZE=1)", path))
    TRUE
  }, error = function(e) {
    message("Skipping ", basename(path), ": ", conditionMessage(e))
    FALSE
  })
  if (exists) {
    message("Using target file: ", basename(path))
    target_files <- c(target_files, path)
  }
}

if (is.null(target_files)) stop("No target files found in S3. Check credentials and bucket paths")

# Read targets via DBI
targets <- lapply(target_files, function(path) {
  DBI::dbGetQuery(con, sprintf("SELECT * FROM read_csv_auto('%s', nullstr='NA')", path)) |>
    dplyr::mutate(site_id = as.character(site_id))  
}) |> bind_rows() |>
  filter(project_id == project, datetime > cut_off_date, !is.na(observation)) |>
  duckdbfs::as_dataset(conn = con)

last_observed_date <- targets |> select(datetime) |> distinct() |>
  filter(datetime == max(datetime)) |> pull(datetime)

DBI::dbExecute(con, paste0(
  "CREATE OR REPLACE VIEW forecast_view AS SELECT * FROM parquet_scan('s3://",
  forecast_bundled_parquet_bucket, "**',
  HIVE_PARTITIONING=TRUE, UNION_BY_NAME=TRUE, FILENAME=FALSE)"
))
# Coastal site_id ("1"/"2") gets inferred as numeric somewhere in the bundling
# pipeline (unlike urban's hyphenated FIPS codes, which are unambiguously
# strings) and comes back out of bundled-parquet as "1.0"/"2.0" -- which then
# fails to match targets' clean "1"/"2" on every join. Strip a trailing ".0"
# unconditionally; it never appears in a legitimate non-coastal site_id.
forecasts <- dplyr::tbl(con, "forecast_view") |>
  mutate(family = ifelse(family == 'ensemble', "sample", family)) |>
  mutate(horizon = date_diff('day', as.POSIXct(reference_datetime), as.POSIXct(datetime))) |>
  filter(!(duration == "P1D" & horizon > 35)) |>
  mutate(site_id = dbplyr::sql("regexp_replace(site_id, '\\.0$', '')"))

scores <- tryCatch(
  open_dataset(paste0("s3://", scores_bundled_parquet_bucket), conn = con) |>
    mutate(site_id = dbplyr::sql("regexp_replace(site_id, '\\.0$', '')")) |>
    filter(project_id == {project},
           datetime > {cut_off_date},
           !is.na(observation)),
  error = function(e) {
    message("No existing scores found, treating as empty: ", e$message)
    dplyr::tibble(
      project_id = character(), site_id = character(),
      datetime = as.POSIXct(character()), duration = character(),
      variable = character(), model_id = character(),
      family = character(), reference_datetime = as.POSIXct(character()),
      observation = numeric()
    ) |> duckdbfs::as_dataset(conn = con)
  }
)

tol <- 1e-2
if (rescore) {
  print("rescoring changed observations")
  scores <- scores |>
    inner_join(targets, by = obs_key_cols) |>
    filter(abs(observation.x - observation.y) / observation.x < {tol})
}

print("Caching forecasts, scores, targets...")

bench::bench_time({
  forecasts |> group_by(variable) |> write_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/forecasts")
})
bench::bench_time({
  scores |> group_by(variable) |> write_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/scores")
})
bench::bench_time({
  targets |> group_by(variable) |> write_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/targets")
})
bench::bench_time({
  forecasts <- open_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/forecasts/**", conn = con)
  scores    <- tryCatch(
    open_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/scores/**", conn = con),
    error = function(e) {
      message("No existing tmp scores found, treating as empty: ", e$message)
      dplyr::tibble(project_id = character(), site_id = character(),
                    datetime = as.POSIXct(character()), duration = character(),
                    variable = character(), model_id = character(),
                    family = character(), reference_datetime = as.POSIXct(character()),
                    observation = numeric()) |>
        duckdbfs::as_dataset(conn = con)
    }
  )
  targets   <- open_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/targets/**", conn = con)
})

print("Compute who needs to be scored...")
bench::bench_time({
  forecasts |>
    anti_join(select(scores, all_of(score_key_cols))) |>
    inner_join(targets) |>
    group_by(variable) |>
    write_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/score_me")
})
