# devtools::install_version("duckdb", "1.2.2")

library(dplyr)
library(duckdbfs)
library(progress)
library(bench)
library(minioclient)
library(yaml)
library(DBI)

install_mc()
mc_alias_set("minio", "uri.osn.mghpcc.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))

config <- read_yaml("challenge_configuration.yaml")
scores_bundled_parquet_bucket <- paste0(config$scores_bucket, "/bundled-parquet/")
project <- config$project_id

setup_s3 <- function(con) {
  DBI::dbExecute(con, paste0("SET s3_access_key_id='",     Sys.getenv("OSN_KEY"),    "';"))
  DBI::dbExecute(con, paste0("SET s3_secret_access_key='", Sys.getenv("OSN_SECRET"), "';"))
  DBI::dbExecute(con, "SET s3_endpoint='uri.osn.mghpcc.org';")
  DBI::dbExecute(con, "SET s3_url_style='path';")
  DBI::dbExecute(con, "SET s3_use_ssl=true;")
  invisible(con)
}

library(score4cast)
con <- duckdbfs::cached_connection(tempfile())
setup_s3(con)

fc <- tryCatch(
  open_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/score_me", conn = con) |>
    filter(!is.na(model_id)),
  error = function(e) {
    message("No score_me files found, nothing to score: ", e$message)
    NULL
  }
)

if (is.null(fc)) {
  message("Exiting early — no forecasts to score.")
  quit(save = "no", status = 0)
}

groups <- fc |> distinct(project_id, duration, variable, model_id, family) |> collect()
total  <- nrow(groups)

if (total == 0) {
  message("No new forecast groups to score.")
  quit(save = "no", status = 0)
}

duckdbfs::close_connection(con)
gc()

score_group <- function(i, groups, project = config$project_id) {

  source("scoring/R/score_joined_table.R")
  con <- duckdbfs::cached_connection(tempfile())
  setup_s3(con)

  fc <- duckdbfs::open_dataset("s3://bu4cast-ci/write/challenges/project_id=bu4cast/tmp/score_me/**",
                               conn = con) |>
    dplyr::filter(!is.na(model_id))

  new_scores <- fc |>
    dplyr::inner_join(groups[i, ], copy = TRUE,
                      by = dplyr::join_by(project_id, duration, variable, model_id, family)) |>
    dplyr::collect() |>
    score_joined_table()

  dur   <- groups$duration[i]
  var   <- groups$variable[i]
  model <- groups$model_id[i]

  path  <- glue::glue("s3://", scores_bundled_parquet_bucket,
                      "project_id={project}/duration={dur}/variable={var}/model_id={model}")
  path2 <- glue::glue("minio/", scores_bundled_parquet_bucket,
                      "project_id={project}/duration={dur}/variable={var}/model_id={model}/")

  message(glue::glue("Joining to existing scores of variable {var} for model {model}"))

  # mc_ls without a trailing slash does a prefix match against sibling
  # entries rather than listing this exact folder's contents -- e.g.
  # "model_id=tg_randforbuoy" (no slash) matches the sibling
  # "model_id=tg_randforbuoystate/" as a false positive, wrongly reporting
  # tg_randforbuoy has existing scores when it has none. The trailing slash
  # above makes this an exact-directory listing.
  file_exist <- length(mc_ls(path2))

  if (file_exist > 0) {
    new_scores      <- duckdbfs::as_dataset(new_scores, conn = con)
    bundled_scores  <- duckdbfs::open_dataset(path, conn = con) |>
      dplyr::anti_join(new_scores,
                       by = dplyr::join_by(reference_datetime, site_id, datetime,
                                           family, pub_datetime, observation,
                                           crps, logs, mean, median, sd,
                                           quantile97.5, quantile02.5, quantile90, quantile10,
                                           duration, model_id, project_id, variable)) |>
      dplyr::compute()
    new_scores <- dplyr::union_all(bundled_scores, new_scores)
  }

  new_scores |>
    dplyr::distinct() |>
    dplyr::group_by(project_id, duration, variable, model_id) |>
    duckdbfs::write_dataset(paste0("s3://", scores_bundled_parquet_bucket))

  duckdbfs::close_connection(con)
  gc()
}

print("Computing new scores....")
pb <- progress_bar$new(format = "  scoring [:bar] :percent in :elapsed",
                       total = total, clear = FALSE, width = 60)

for (i in seq_len(nrow(groups))) {
  pb$tick()
  print(paste("Scoring model:", groups$model_id[i], "variable:", groups$variable[i]))
  score_group(i, groups)
}
