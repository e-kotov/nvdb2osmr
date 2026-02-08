#' Convert NVDB data to OSM PBF using parallel processing (WKB optimized)
#'
#' @param input_path Path to input file (.gdb, .gpkg, or .geoparquet)
#' @param output_pbf Path to final output .osm.pbf
#' @param municipality_codes Optional vector of municipality codes to process (default: all)
#' @param use_geoparquet Use GeoParquet for faster processing: "auto", TRUE, or FALSE (default: "auto")
#' @param presplit Logical: whether to pre-split to temp files (default: FALSE)
#' @param max_retries Maximum retries for failed municipalities (default: 2)
#' @param duckdb_memory_limit Memory limit for DuckDB (e.g., "10GB"). Default "4GB".
#' @param duckdb_threads Number of threads for DuckDB. Default 1 (ideal for parallel runs).
#' @details 
#' This function supports parallel processing via the \code{mirai} package. 
#' To run in parallel, you must set up mirai daemons before calling this function, 
#' for example using \code{mirai::daemons(n_workers)}. 
#' If no daemons are active, processing will happen sequentially.
#' @return Path to output PBF file (invisibly)
#' @export
nvdb_to_pbf <- function(
  input_path,
  output_pbf,
  municipality_codes = NULL,
  use_geoparquet = "auto",
  presplit = FALSE,
  max_retries = 2,
  duckdb_memory_limit = "4GB",
  duckdb_threads = 1
) {
  # --- Input Validation ---
  if (!is.character(input_path) || length(input_path) != 1) {
    stop("input_path must be a single character string")
  }
  if (!file.exists(input_path)) {
    stop("Input file not found: ", input_path)
  }

  if (!is.character(output_pbf) || length(output_pbf) != 1) {
    stop("output_pbf must be a single character string")
  }
  output_dir <- dirname(output_pbf)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # Ensure mirai is available
  if (!requireNamespace("mirai", quietly = TRUE)) {
    stop(
      "Package 'mirai' is required for parallel processing. Please install it."
    )
  }

  # Check for active mirai daemons
  status <- mirai::status()
  daemons_active <- is.numeric(status$daemons) && status$daemons > 0

  # --- 0. RESOLVE SOURCE (GDB/GPKG -> GeoParquet if needed) ---
  source_info <- resolve_source(
    input_path,
    use_geoparquet,
    output_pbf,
    duckdb_memory_limit = duckdb_memory_limit,
    duckdb_threads = duckdb_threads,
    verbose = TRUE
  )
  gdb_path <- source_info$path # This is the path to use (may be GeoParquet)

  # --- 1. DISCOVERY ---
  cli::cli_inform("Discovering municipalities...")

  cli::cli_inform("  - Connecting to DuckDB...")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(if (!is.null(con)) DBI::dbDisconnect(con))

  # Set resource limits for discovery
  DBI::dbExecute(con, sprintf("SET memory_limit = '%s';", duckdb_memory_limit))
  DBI::dbExecute(con, sprintf("SET threads = %d;", duckdb_threads))

  cli::cli_inform("  - Loading spatial extension...")
  tryCatch({
    DBI::dbExecute(con, "LOAD spatial;")
  }, error = function(e) {
    tryCatch({
      DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")
    }, error = function(e2) {
      stop("Failed to load DuckDB 'spatial' extension. Please ensure it is installed: ", conditionMessage(e2))
    })
  })

  # Determine table reference based on source type
  is_geoparquet_source <- source_info$is_geoparquet
  if (is_geoparquet_source) {
    # Parquet is built-in, no LOAD/INSTALL needed
    table_ref <- sprintf("read_parquet('%s')", gdb_path)
  } else {
    table_ref <- sprintf("ST_Read('%s')", gdb_path)
  }

  if (is.null(municipality_codes)) {
    cli::cli_inform(
      "  - Querying unique municipality codes from {table_ref}..."
    )
    codes <- DBI::dbGetQuery(
      con,
      sprintf("SELECT DISTINCT Kommu_141 FROM %s ORDER BY Kommu_141", table_ref)
    )$Kommu_141
  } else {
    # Validate municipality codes
    codes <- as.character(municipality_codes)
    if (any(!grepl("^[0-9]+$", codes))) {
      stop("All municipality_codes must be numeric strings (e.g., '2480')")
    }
  }

  cli::cli_inform("Found {length(codes)} municipality{?ies} to process")

  # --- 2. PRE-SPLIT (optional) ---
  temp_dir <- NULL
  source_files <- list()
  counts <- NULL

  if (presplit) {
    cli::cli_inform("Pre-splitting to individual municipality files...")
    temp_dir <- tempfile(pattern = "nvdb_split_")
    dir.create(temp_dir, showWarnings = FALSE)

    # Get segment counts for LPT scheduling
    counts <- DBI::dbGetQuery(
      con,
      sprintf(
        "
      SELECT Kommu_141, COUNT(*) as n 
      FROM %s 
      GROUP BY Kommu_141
    ",
        table_ref
      )
    )

    # Sort by count descending (LPT scheduling - largest first)
    counts <- counts[order(-counts$n), ]
    codes <- counts$Kommu_141

    cli::cli_inform(
      "Largest municipality: {codes[1]} ({format(counts$n[1], big.mark = ',')} segments)"
    )

    # Export each municipality to separate GeoPackage
    for (code in codes) {
      out_file <- file.path(temp_dir, sprintf("muni_%s.gpkg", code))
      # Use table_ref for the source (works for both GeoParquet and GDB/GPKG)
      query <- sprintf(
        "
        COPY (
          SELECT * FROM %s 
          WHERE Kommu_141 = '%s'
        ) TO '%s' WITH (FORMAT GDAL, DRIVER 'GPKG')
      ",
        table_ref,
        code,
        out_file
      )

      tryCatch(
        {
          DBI::dbExecute(con, query)
          source_files[[code]] <- out_file
        },
        error = function(e) {
          cli::cli_warn("Failed to export {code}: {e$message}")
        }
      )
    }

    cli::cli_inform(
      "Pre-split {length(source_files)}/{length(codes)} municipalities"
    )
  }

  DBI::dbDisconnect(con)
  con <- NULL

  # --- 3. ID RANGE ASSIGNMENT ---
  # 10M IDs per chunk (confirmed safe: largest county < 3M IDs)
  chunk_configs <- lapply(seq_along(codes), function(i) {
    list(
      code = codes[i],
      node_id_start = (i - 1) * 10000000 + 1,
      way_id_start = (i - 1) * 10000000 + 1,
      source_file = if (presplit) source_files[[codes[i]]] else NULL
    )
  })

  # --- 4. EXECUTION ---
  # Define worker function
  process_municipality <- function(cfg, main_gdb, mem_limit, threads) {
    code <- cfg$code
    source_file <- cfg$source_file

    # Determine which file to use
    if (!is.null(source_file)) {
      gdb_to_use <- source_file
    } else {
      gdb_to_use <- main_gdb
    }

    # Output file for this chunk
    chunk_file <- tempfile(
      pattern = paste0("nvdb_", code, "_"),
      fileext = ".osm.pbf"
    )

    tryCatch(
      {
        # Use the new WKB-optimized function with verbose=FALSE for cleaner parallel output
        process_nvdb_fast(
          gdb_path = gdb_to_use,
          output_pbf = chunk_file,
          municipality_code = code,
          simplify_method = "refname",
          node_id_start = cfg$node_id_start,
          way_id_start = cfg$way_id_start,
          duckdb_memory_limit = mem_limit,
          duckdb_threads = threads,
          verbose = FALSE
        )

        list(code = code, file = chunk_file, n_segments = NA, success = TRUE)
      },
      error = function(e) {
        list(
          code = code,
          file = NULL,
          n_segments = 0,
          success = FALSE,
          error = conditionMessage(e)
        )
      }
    )
  }

  if (daemons_active) {
    # PARALLEL PATH
    cli::cli_inform("Using {status$daemons} active mirai worker{?s}...")

    # Export common data to all workers ONCE
    mirai::everywhere({
      library(nvdb2osmr)
    })

    cli::cli_inform("Processing municipalities in parallel...")
    start_time <- Sys.time()

    all_results <- list()
    pending_configs <- chunk_configs
    attempt <- 1

    while (length(pending_configs) > 0 && attempt <= max_retries) {
      if (attempt > 1) {
        cli::cli_inform(
          "Retry attempt {attempt}/{max_retries} for {length(pending_configs)} failed municipality{?ies}..."
        )
      }

      # Launch async tasks with progress bar
      mirai_res <- mirai::mirai_map(
        pending_configs,
        process_municipality,
        .args = list(
          main_gdb = gdb_path,
          mem_limit = duckdb_memory_limit,
          threads = duckdb_threads
        ),
        .progress = TRUE
      )

      # Collect results
      results <- mirai_res[]

      # Separate successful and failed
      successful_this_round <- results[sapply(results, function(x) {
        is.list(x) && isTRUE(x$success) && !is.null(x$file)
      })]
      failed_this_round <- results[
        !sapply(results, function(x) is.list(x) && isTRUE(x$success))
      ]

      # Store successful results
      all_results <- c(all_results, successful_this_round)

      # Prepare for retry
      if (length(failed_this_round) > 0) {
        failed_codes <- sapply(failed_this_round, function(x) {
          if (is.list(x)) x$code else NA
        })
        failed_codes <- failed_codes[!is.na(failed_codes)]
        pending_configs <- chunk_configs[sapply(chunk_configs, function(x) {
          x$code %in% failed_codes
        })]

        if (length(pending_configs) > 0 && attempt < max_retries) {
          cli::cli_warn(
            "{length(pending_configs)} municipality{?ies} failed, will retry: {paste(failed_codes, collapse = ', ')}"
          )
          Sys.sleep(1) # Brief pause before retry
        }
      } else {
        pending_configs <- list()
      }

      attempt <- attempt + 1
    }
  } else {
    # SEQUENTIAL PATH (No active daemons)
    cli::cli_inform("No active mirai daemons found. Processing {length(codes)} municipalities sequentially...")
    start_time <- Sys.time()
    
    all_results <- lapply(chunk_configs, function(cfg) {
      process_municipality(
        cfg = cfg, 
        main_gdb = gdb_path, 
        mem_limit = duckdb_memory_limit,
        threads = duckdb_threads
      )
    })
  }

  elapsed <- difftime(Sys.time(), start_time, units = "mins")

  # --- 7. REPORT & MERGE ---
  successful <- all_results[sapply(all_results, function(x) {
    is.list(x) && isTRUE(x$success) && !is.null(x$file)
  })]
  failed <- all_results[
    !sapply(all_results, function(x) is.list(x) && isTRUE(x$success))
  ]

  if (length(failed) > 0) {
    failed_codes <- sapply(failed, function(x) {
      if (is.list(x) && !is.null(x$code)) x$code else "unknown"
    })
    cli::cli_warn(
      "{length(failed)} municipality{?ies} failed after {max_retries} attempt{?s}: {paste(failed_codes, collapse = ', ')}"
    )
    for (f in failed) {
      if (is.list(f)) {
        err <- if (!is.null(f$error)) f$error else "Unknown error"
        cli::cli_text("  {.code {f$code %||% 'unknown'}}: {err}")
      }
    }
  }

  cli::cli_inform("")
  cli::cli_alert_success(
    "Completed {length(successful)}/{length(codes)} municipalities in {round(as.numeric(elapsed), 1)} minutes"
  )

  if (length(successful) == 0) {
    stop("No municipalities were successfully processed")
  }

  # Collect chunk files
  chunk_files <- unlist(lapply(successful, function(x) x$file))
  chunk_files <- chunk_files[!is.null(chunk_files) & file.exists(chunk_files)]

  if (length(chunk_files) == 0) {
    stop("No valid chunk files to merge")
  }

  # Merge and sort chunks (osmium sort handles both merging and ID ordering)
  if (length(chunk_files) == 1) {
    cli::cli_inform("Copying single output file...")
    file.copy(chunk_files[1], output_pbf, overwrite = TRUE)
  } else {
    cli::cli_inform(
      "Merging and sorting {length(chunk_files)} chunks to final PBF..."
    )

    if (requireNamespace("rosmium", quietly = TRUE)) {
      rosmium::osm_sort(
        input_paths = chunk_files,
        output_path = output_pbf,
        overwrite = TRUE,
        echo = FALSE,
        spinner = FALSE
      )
    } else {
      # Fallback to command line osmium
      cmd <- sprintf(
        "osmium sort %s -o %s --overwrite",
        paste(shQuote(chunk_files), collapse = " "),
        shQuote(output_pbf)
      )
      system(cmd)
    }
  }

  # Cleanup temp files
  unlink(chunk_files)
  if (!is.null(temp_dir)) {
    unlink(temp_dir, recursive = TRUE)
  }

  # Final stats
  if (file.exists(output_pbf)) {
    size_mb <- file.size(output_pbf) / 1e6
    cli::cli_alert_success(
      "Done! Output: {.file {output_pbf}} ({round(size_mb, 2)} MB)"
    )
  } else {
    cli::cli_alert_success("Done! Output: {.file {output_pbf}}")
  }

  invisible(output_pbf)
}

#' Helper for NULL default
#' @noRd
#' @keywords internal
`%||%` <- function(x, y) if (is.null(x)) y else x
