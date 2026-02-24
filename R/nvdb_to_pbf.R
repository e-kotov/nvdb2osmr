# Internal helper to interpret mirai daemon state for branching and logging.
resolve_mirai_state <- function(daemons_configured, info) {
  daemons_configured <- isTRUE(daemons_configured)

  if (!daemons_configured) {
    return(list(
      daemons_configured = FALSE,
      n_workers = 0L
    ))
  }

  connections <- tryCatch(
    info[["connections"]],
    error = function(...) NULL
  )
  n_workers <- if (
    is.numeric(connections) &&
      length(connections) == 1L &&
      !is.na(connections) &&
      connections >= 0
  ) {
    as.integer(connections)
  } else {
    0L
  }

  list(
    daemons_configured = TRUE,
    n_workers = n_workers
  )
}

build_area_where_sql <- function(con, municipality_codes, county_codes) {
  if (!is.null(municipality_codes)) {
    return(glue::glue_sql(
      "WHERE Kommu_141 IN ({municipality_codes*})",
      .con = con
    ))
  }

  if (!is.null(county_codes)) {
    like_exprs <- lapply(county_codes, function(x) {
      glue::glue_sql("Kommu_141 LIKE {paste0(x, '%')}", .con = con)
    })
    return(glue::glue_sql(
      "WHERE ({glue::glue_sql_collapse(like_exprs, sep = ' OR ')})",
      .con = con
    ))
  }

  DBI::SQL("")
}

detect_prepass_geometry_expr <- function(con, source_path, is_geoparquet_source) {
  if (is_geoparquet_source) {
    all_cols <- DBI::dbGetQuery(
      con,
      glue::glue_sql(
        "SELECT * FROM read_parquet({source_path}) LIMIT 0",
        .con = con
      )
    )
    available_cols <- names(all_cols)

    geom_col <- NULL
    if ("geometry" %in% available_cols) {
      geom_col <- "geometry"
    } else if ("geom_wkb" %in% available_cols) {
      geom_col <- "geom_wkb"
    } else {
      for (col in available_cols) {
        test <- tryCatch(
          DBI::dbGetQuery(
            con,
            glue::glue_sql(
              "SELECT ST_GeometryType({`col`}) as gtype FROM read_parquet({source_path}) WHERE {`col`} IS NOT NULL LIMIT 1",
              .con = con
            )
          ),
          error = function(...) NULL
        )
        if (!is.null(test) && nrow(test) > 0 && !is.na(test$gtype[1])) {
          geom_col <- col
          break
        }
      }
    }

    if (is.null(geom_col)) {
      stop(
        "Could not detect geometry column for global node prepass in ",
        source_path
      )
    }

    type_info <- DBI::dbGetQuery(
      con,
      glue::glue_sql(
        "DESCRIBE SELECT {`geom_col`} FROM read_parquet({source_path})",
        .con = con
      )
    )
    actual_type <- toupper(type_info$column_type[1])

    if (actual_type == "GEOMETRY") {
      return(glue::glue_sql("{`geom_col`}", .con = con))
    }
    return(glue::glue_sql("ST_GeomFromWKB({`geom_col`})", .con = con))
  }

  all_cols <- DBI::dbGetQuery(
    con,
    glue::glue_sql("SELECT * FROM ST_Read({source_path}) LIMIT 0", .con = con)
  )
  available_cols <- names(all_cols)

  geom_col <- NULL
  if ("Shape" %in% available_cols) {
    geom_col <- "Shape"
  } else if ("geom" %in% available_cols) {
    geom_col <- "geom"
  } else {
    for (col in available_cols) {
      test <- tryCatch(
        DBI::dbGetQuery(
          con,
          glue::glue_sql(
            "SELECT ST_GeometryType({`col`}) as gtype FROM ST_Read({source_path}) WHERE {`col`} IS NOT NULL LIMIT 1",
            .con = con
          )
        ),
        error = function(...) NULL
      )
      if (!is.null(test) && nrow(test) > 0 && !is.na(test$gtype[1])) {
        geom_col <- col
        break
      }
    }
  }

  if (is.null(geom_col)) {
    stop(
      "Could not detect geometry column for global node prepass in ",
      source_path
    )
  }

  sweref99_tm <- "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  wgs84 <- "+proj=longlat +datum=WGS84 +no_defs"

  glue::glue_sql(
    "ST_Transform(ST_Force2D({`geom_col`}), {sweref99_tm}, {wgs84})",
    .con = con
  )
}

build_global_node_prepass_dictionary <- function(
  con,
  table_ref,
  where_sql,
  area_code_col,
  geometry_expr,
  dict_dir
) {
  dict_path <- file.path(dict_dir, "global_node_dictionary.parquet")

  dict_query <- glue::glue_sql(
    "
    WITH endpoints AS (
      SELECT
        CAST({area_code_col} AS VARCHAR) AS area_code,
        CAST(ROUND(ST_X(ST_StartPoint(ST_LineMerge({geometry_expr}))) * 10000000) AS BIGINT) AS sx7,
        CAST(ROUND(ST_Y(ST_StartPoint(ST_LineMerge({geometry_expr}))) * 10000000) AS BIGINT) AS sy7,
        CAST(ROUND(ST_X(ST_EndPoint(ST_LineMerge({geometry_expr}))) * 10000000) AS BIGINT) AS ex7,
        CAST(ROUND(ST_Y(ST_EndPoint(ST_LineMerge({geometry_expr}))) * 10000000) AS BIGINT) AS ey7
      FROM {table_ref}
      {where_sql}
    ),
    points AS (
      SELECT area_code, sx7 AS x7, sy7 AS y7 FROM endpoints
      UNION ALL
      SELECT area_code, ex7 AS x7, ey7 AS y7 FROM endpoints
    ),
    filtered AS (
      SELECT area_code, x7, y7
      FROM points
      WHERE x7 IS NOT NULL AND y7 IS NOT NULL
    ),
    grouped AS (
      SELECT
        x7,
        y7,
        MIN(area_code) AS owner_area,
        COUNT(DISTINCT area_code) AS area_count
      FROM filtered
      GROUP BY x7, y7
    )
    SELECT
      x7,
      y7,
      CAST(1000000000000 + ROW_NUMBER() OVER (ORDER BY x7, y7) AS BIGINT) AS global_node_id,
      owner_area,
      area_count
    FROM grouped
    ",
    .con = con
  )

  DBI::dbExecute(
    con,
    glue::glue_sql(
      "COPY ({dict_query}) TO {dict_path} (FORMAT PARQUET)",
      .con = con
    )
  )

  stats <- DBI::dbGetQuery(
    con,
    glue::glue_sql(
      "
      SELECT
        COUNT(*) AS n_total,
        SUM(CASE WHEN area_count > 1 THEN 1 ELSE 0 END) AS n_shared,
        MAX(area_count) AS max_area_count
      FROM read_parquet({dict_path})
      ",
      .con = con
    )
  )

  list(path = dict_path, stats = stats[1, ])
}

#' Convert NVDB data to OSM PBF using parallel processing (WKB optimized)
#'
#' @param input_path Path to input file (.gdb, .gpkg, or .geoparquet)
#' @param output_pbf Path to final output .osm.pbf
#' @param municipality_codes Optional vector of 4-digit municipality codes to process (default: all)
#' @param county_codes Optional vector of 2-digit county codes to process (e.g., "01" for Stockholm)
#' @param split_by Strategy for splitting the work: "municipality" (default), "county", or "none" (process whole file in one go).
#' @param use_geoparquet Use GeoParquet for faster processing: "auto", TRUE, or FALSE (default: "auto")
#' @param global_node_prepass Whether to build a global endpoint-node dictionary before split processing.
#'   One of "auto" (default), "on", or "off".
#'   For split processing (`split_by = "municipality"` or `"county"`), `"off"` is not allowed
#'   and raises an error because split mode requires global node prepass for boundary-safe connectivity.
#' @param presplit Logical: whether to pre-split to temp files (default: FALSE)
#' @param max_retries Maximum retries for failed municipalities (default: 2)
#' @param duckdb_memory_limit_gb Memory limit for DuckDB in GB (numeric). Default 4.
#' @param duckdb_threads Number of threads for DuckDB. Default 1 (ideal for parallel runs).
#' @details 
#' This function supports parallel processing via the \code{mirai} package. 
#' To run in parallel, you must set up mirai daemons before calling this function, 
#' for example using \code{mirai::daemons(4)}. 
#' To shut down daemons after processing, call \code{mirai::daemons(0)}.
#' If no daemons are configured, processing will happen sequentially.
#' 
#' Splitting by "municipality" is recommended for high-core counts as it provides 
#' more granular tasks (~290 tasks). "county" provides ~21 tasks. 
#' "none" handles everything in a single process (memory intensive for large areas).
#' @return Path to output PBF file (invisibly)
#' @export
nvdb_to_pbf <- function(
  input_path,
  output_pbf,
  municipality_codes = NULL,
  county_codes = NULL,
  split_by = c("municipality", "county", "none"),
  use_geoparquet = "auto",
  global_node_prepass = c("auto", "on", "off"),
  simplify_method = "refname",
  presplit = FALSE,
  max_retries = 2,
  duckdb_memory_limit_gb = 4,
  duckdb_threads = 1
) {
  split_by <- match.arg(split_by)
  global_node_prepass <- match.arg(global_node_prepass)

  # Guardrail: split mode requires global prepass to avoid split-induced
  # boundary node duplication and disconnected graph artifacts.
  if (split_by != "none" && global_node_prepass == "off") {
    stop(
      "global_node_prepass='off' is not allowed when split_by='",
      split_by,
      "'. Split processing requires global node prepass."
    )
  }
  if (split_by == "none" && global_node_prepass != "auto") {
    cli::cli_warn(
      "global_node_prepass='{global_node_prepass}' is ignored when split_by='none'"
    )
  }

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

  if (!is.numeric(duckdb_memory_limit_gb)) {
    stop("duckdb_memory_limit_gb must be a number (Gigabytes)")
  }

  # Ensure mirai is available
  if (!requireNamespace("mirai", quietly = TRUE)) {
    stop(
      "Package 'mirai' is required for parallel processing. Please install it."
    )
  }

  # Check configured mirai state via package-author interfaces.
  mirai_state <- resolve_mirai_state(
    daemons_configured = mirai::daemons_set(),
    info = mirai::info()
  )
  daemons_configured <- mirai_state$daemons_configured
  n_workers <- mirai_state$n_workers

  # --- 0. RESOLVE SOURCE (GDB/GPKG -> GeoParquet if needed) ---
  source_info <- resolve_source(
    input_path,
    use_geoparquet,
    output_pbf,
    duckdb_memory_limit_gb = duckdb_memory_limit_gb,
    duckdb_threads = duckdb_threads,
    verbose = TRUE
  )
  gdb_path <- source_info$path # This is the path to use (may be GeoParquet)
  use_global_prepass <- FALSE
  global_prepass_dir <- NULL
  global_node_dict_path <- NULL

  # --- 1. DISCOVERY ---
  temp_dir <- NULL
  if (split_by != "none") {
    use_global_prepass <- TRUE

    cli::cli_inform("Discovering areas to process...")

    cli::cli_inform("  - Connecting to DuckDB...")
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(if (!is.null(con)) DBI::dbDisconnect(con))

    # Set resource limits for discovery
    limit_str <- paste0(as.integer(duckdb_memory_limit_gb), "GB")
    DBI::dbExecute(con, glue::glue_sql("SET memory_limit = {limit_str};", .con = con))
    DBI::dbExecute(con, glue::glue_sql("SET threads = 1;", .con = con))

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
      table_ref <- glue::glue_sql("read_parquet({gdb_path})", .con = con)
    } else {
      table_ref <- glue::glue_sql("ST_Read({gdb_path})", .con = con)
    }

    # Build the code selection logic
    if (split_by == "municipality") {
      code_col <- DBI::SQL("Kommu_141")
      label <- "municipality"
    } else {
      # County code is the first 2 digits of the municipality code
      code_col <- DBI::SQL("SUBSTR(Kommu_141, 1, 2)")
      label <- "county"
    }

    # Apply filters
    where_sql <- build_area_where_sql(con, municipality_codes, county_codes)

    query <- glue::glue_sql("SELECT DISTINCT {code_col} as area_code FROM {table_ref} {where_sql} ORDER BY area_code", 
                           .con = con)
    
    cli::cli_inform("  - Querying unique {label} codes...")
    codes <- DBI::dbGetQuery(con, query)$area_code
    
    if (length(codes) == 0) {
      stop("No areas found matching the criteria.")
    }

    cli::cli_inform("Found {length(codes)} {label}{?s} to process")

    # --- 2. PRE-SPLIT (optional) ---
    temp_dir <- NULL
    source_files <- list()
    counts <- NULL

    if (presplit) {
      cli::cli_inform("Pre-splitting to individual area files...")
      temp_dir <- tempfile(pattern = "nvdb_split_")
      dir.create(temp_dir, showWarnings = FALSE)

      # Get segment counts for LPT scheduling
      count_query <- glue::glue_sql("SELECT {code_col} as area_code, COUNT(*) as n FROM {table_ref} {where_sql} GROUP BY area_code",
                                   .con = con)
      counts <- DBI::dbGetQuery(con, count_query)

      # Sort by count descending (LPT scheduling - largest first)
      counts <- counts[order(-counts$n), ]
      codes <- counts$area_code

      cli::cli_inform(
        "Largest {label}: {codes[1]} ({format(counts$n[1], big.mark = ',')} segments)"
      )

      # Export each area to separate GeoPackage
      for (code in codes) {
        out_file <- file.path(temp_dir, sprintf("area_%s.gpkg", code))
        
        area_filter <- if (split_by == "municipality") {
          glue::glue_sql("Kommu_141 = {code}", .con = con)
        } else {
          glue::glue_sql("Kommu_141 LIKE {paste0(code, '%')}", .con = con)
        }

        query <- glue::glue_sql(
          "COPY (SELECT * FROM {table_ref} WHERE {area_filter}) TO {out_file} WITH (FORMAT GDAL, DRIVER 'GPKG')",
          .con = con
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
        "Pre-split {length(source_files)}/{length(codes)} areas"
      )
    }

    if (use_global_prepass) {
      cli::cli_inform("Building global endpoint-node dictionary...")
      global_prepass_dir <- tempfile(pattern = "nvdb_prepass_")
      dir.create(global_prepass_dir, showWarnings = FALSE)

      geometry_expr <- detect_prepass_geometry_expr(
        con = con,
        source_path = gdb_path,
        is_geoparquet_source = is_geoparquet_source
      )
      dict_info <- build_global_node_prepass_dictionary(
        con = con,
        table_ref = table_ref,
        where_sql = where_sql,
        area_code_col = code_col,
        geometry_expr = geometry_expr,
        dict_dir = global_prepass_dir
      )
      global_node_dict_path <- dict_info$path

      cli::cli_inform(
        paste0(
          "Global node dictionary: ",
          format(dict_info$stats$n_total, big.mark = ","),
          " unique endpoint keys, ",
          format(dict_info$stats$n_shared, big.mark = ","),
          " shared across areas (max ",
          dict_info$stats$max_area_count,
          " areas)"
        )
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
        source_file = if (presplit) source_files[[codes[i]]] else NULL,
        split_by = split_by,
        global_node_dict_path = global_node_dict_path
      )
    })
  } else {
    use_global_prepass <- FALSE

    # split_by == "none"
    codes <- "all"
    chunk_configs <- list(list(
      code = "sweden",
      node_id_start = 1,
      way_id_start = 1,
      source_file = NULL,
      split_by = "none",
      global_node_dict_path = NULL
    ))
    label <- "file"
  }

  # --- 4. EXECUTION ---
  # Define worker function
  process_area <- function(cfg, main_gdb, mem_limit_gb, threads) {
    code <- cfg$code
    source_file <- cfg$source_file
    dict_path <- cfg$global_node_dict_path

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
          municipality_code = if (cfg$split_by == "municipality") code else NULL,
          county_code = if (cfg$split_by == "county") code else NULL,
          simplify_method = simplify_method,
          node_id_start = cfg$node_id_start,
          way_id_start = cfg$way_id_start,
          global_node_dict_path = dict_path,
          area_code = if (!is.null(dict_path)) code else NULL,
          prepass_rounding = "duckdb_1e7",
          duckdb_memory_limit_gb = mem_limit_gb,
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

  if (daemons_configured) {
    # PARALLEL PATH
    if (n_workers > 0) {
      cli::cli_inform("Using {n_workers} active mirai worker{?s}...")
    } else {
      cli::cli_inform("Using configured mirai daemons in sync mode...")
    }

    # Export common data to all workers ONCE
    mirai::everywhere({
      library(nvdb2osmr)
    })

    cli::cli_inform("Processing areas in parallel...")
    start_time <- Sys.time()

    all_results <- list()
    pending_configs <- chunk_configs
    attempt <- 1

    while (length(pending_configs) > 0 && attempt <= max_retries) {
      if (attempt > 1) {
        cli::cli_inform(
          "Retry attempt {attempt}/{max_retries} for {length(pending_configs)} failed area{?s}..."
        )
      }

      # Launch async tasks with progress bar
      mirai_res <- mirai::mirai_map(
        pending_configs,
        process_area,
        .args = list(
          main_gdb = gdb_path,
          mem_limit_gb = duckdb_memory_limit_gb,
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
            "{length(pending_configs)} area{?s} failed, will retry: {paste(failed_codes, collapse = ', ')}"
          )
          Sys.sleep(1) # Brief pause before retry
        }
      } else {
        pending_configs <- list()
      }

      attempt <- attempt + 1
    }
  } else {
    # SEQUENTIAL PATH (No configured daemons)
    cli::cli_inform("No active mirai daemons found. Processing {length(codes)} areas sequentially...")
    start_time <- Sys.time()
    
    all_results <- lapply(chunk_configs, function(cfg) {
      process_area(
        cfg = cfg, 
        main_gdb = gdb_path, 
        mem_limit_gb = duckdb_memory_limit_gb,
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
      "{length(failed)} area{?s} failed after {max_retries} attempt{?s}: {paste(failed_codes, collapse = ', ')}"
    )
    for (f in failed) {
      if (is.list(f)) {
        err <- if (!is.null(f$error)) f$error else "Unknown error"
        cli::cli_text("  {.code {f$code %||% 'unknown'}}: {err}")
      }
    }

    if (use_global_prepass) {
      failed_codes <- sapply(failed, function(x) {
        if (is.list(x) && !is.null(x$code)) x$code else "unknown"
      })
      successful_files <- unlist(lapply(successful, function(x) x$file))
      if (length(successful_files) > 0) {
        unlink(successful_files[file.exists(successful_files)])
      }
      if (!is.null(temp_dir)) {
        unlink(temp_dir, recursive = TRUE)
      }
      if (!is.null(global_prepass_dir)) {
        unlink(global_prepass_dir, recursive = TRUE)
      }
      stop(
        "Global node prepass requires all areas to succeed. Aborting due to failed areas: ",
        paste(failed_codes, collapse = ", ")
      )
    }
  }

  cli::cli_inform("")
  cli::cli_alert_success(
    "Completed {length(successful)}/{length(codes)} areas in {round(as.numeric(elapsed), 1)} minutes"
  )

  if (length(successful) == 0) {
    stop("No areas were successfully processed")
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
  if (!is.null(global_prepass_dir)) {
    unlink(global_prepass_dir, recursive = TRUE)
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
