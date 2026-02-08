#' GeoParquet Conversion Utilities
#'
#' Internal functions for converting NVDB data to GeoParquet format
#' for efficient filter pushdown during parallel processing.
#'
#' @noRd
#' @keywords internal

#' Check if GeoParquet file is current relative to source
#' @param source_path Path to source file (GDB or GPKG)
#' @param geoparquet_path Path to GeoParquet file
#' @return Logical indicating if GeoParquet is up to date
#' @noRd
is_geoparquet_current <- function(source_path, geoparquet_path) {
  if (!file.exists(geoparquet_path)) {
    return(FALSE)
  }
  
  metadata_path <- paste0(geoparquet_path, ".meta")
  if (!file.exists(metadata_path)) {
    # No metadata, assume outdated
    return(FALSE)
  }
  
  metadata <- jsonlite::fromJSON(metadata_path)
  source_mtime <- file.info(source_path)$mtime
  
  # Check if source is newer than when GeoParquet was created
  as.POSIXct(metadata$source_mtime) >= source_mtime
}

#' Ensure GeoParquet file exists, creating if necessary
#' @param source_path Path to source file (GDB or GPKG)
#' @param geoparquet_path Path to desired GeoParquet output
#' @param verbose Print progress messages
#' @return Path to GeoParquet file (invisibly)
#' @noRd
ensure_geoparquet <- function(source_path, geoparquet_path, verbose = TRUE) {
  if (file.exists(geoparquet_path) && is_geoparquet_current(source_path, geoparquet_path)) {
    if (verbose) {
      cli::cli_alert_success("Using existing GeoParquet: {.file {geoparquet_path}}")
    }
    return(invisible(geoparquet_path))
  }
  
  if (verbose) {
    cli::cli_alert_info("One-time optimization: Converting to GeoParquet for faster processing...")
    cli::cli_text("This may take 1-3 minutes but makes subsequent runs {.strong 10x faster}.")
  }
  
  start_time <- Sys.time()
  
  # Perform conversion
  convert_to_geoparquet(source_path, geoparquet_path, verbose)
  
  # Write metadata
  metadata <- list(
    source_file = normalizePath(source_path),
    source_mtime = as.character(file.info(source_path)$mtime),
    created = as.character(Sys.time())
  )
  
  jsonlite::write_json(metadata, paste0(geoparquet_path, ".meta"))
  
  elapsed <- difftime(Sys.time(), start_time, units = "secs")
  
  if (verbose) {
    file_size <- file.size(geoparquet_path) / 1e6
    source_size <- sum(file.info(list.files(source_path, full.names = TRUE, recursive = TRUE))$size, na.rm = TRUE) / 1e6
    cli::cli_alert_success("GeoParquet created in {round(as.numeric(elapsed))}s ({round(file_size, 1)} MB, {round(source_size/file_size, 1)}x smaller)")
  }
  
  invisible(geoparquet_path)
}

#' Convert source file to GeoParquet with pre-transformed WGS84 geometries
#' @param source_path Path to source file (GDB or GPKG)
#' @param geoparquet_path Path to output GeoParquet
#' @param verbose Print progress messages
#' @noRd
convert_to_geoparquet <- function(source_path, geoparquet_path, verbose = TRUE) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  tryCatch({
    DBI::dbExecute(con, "LOAD spatial;")
  }, error = function(e) {
    tryCatch({
      DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")
    }, error = function(e2) {
      stop("Failed to load DuckDB 'spatial' extension: ", conditionMessage(e2))
    })
  })
  
  # PROJ strings for transformation
  sweref99_tm <- "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  wgs84 <- "+proj=longlat +datum=WGS84 +no_defs"
  
  # Get available columns to detect geometry and handles EXCLUDE
  all_cols <- DBI::dbGetQuery(con, sprintf("SELECT * FROM ST_Read('%s') LIMIT 0", source_path))
  available_cols <- names(all_cols)
  
  # Auto-detect geometry column
  geom_col <- NULL
  if ("Shape" %in% available_cols) {
    geom_col <- "Shape"
  } else if ("geom" %in% available_cols) {
    geom_col <- "geom"
  } else {
    # Try to find any geometry column
    for (col in available_cols) {
      test <- tryCatch({
        DBI::dbGetQuery(con, sprintf("
          SELECT ST_GeometryType(\"%s\") as gtype 
          FROM ST_Read('%s') 
          WHERE \"%s\" IS NOT NULL 
          LIMIT 1
        ", col, source_path, col))
      }, error = function(e) NULL)
      if (!is.null(test) && nrow(test) > 0 && !is.na(test$gtype[1])) {
        geom_col <- col
        break
      }
    }
  }
  
  if (is.null(geom_col)) {
    stop("Could not detect geometry column in source: ", source_path)
  }

  # Build query - transform to WGS84 during conversion
  # Note: ST_AsWKB is used to ensure the Parquet file contains standard WKB blobs.
  # We exclude the original geometry and Shape_Length if it exists.
  exclude_list <- unique(c(geom_col, "Shape_Length"))
  exclude_list <- exclude_list[exclude_list %in% available_cols]
  exclude_clause <- if (length(exclude_list) > 0) {
    sprintf("EXCLUDE(%s)", paste(sprintf('"%s"', exclude_list), collapse = ", "))
  } else {
    ""
  }

  query <- sprintf("
    COPY (
      SELECT 
        ST_AsWKB(ST_Transform(ST_Force2D(\"%s\"), '%s', '%s')) as geom_wkb,
        * %s
      FROM ST_Read('%s')
    ) TO '%s' (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
  ", geom_col, sweref99_tm, wgs84, exclude_clause, source_path, geoparquet_path)
  
  if (verbose) {
    cli::cli_alert_info("Converting {basename(source_path)} to GeoParquet...")
  }
  
  DBI::dbExecute(con, query)
  
  invisible(geoparquet_path)
}

#' Resolve input source - handles GDB, GPKG, GeoParquet with auto-conversion
#' 
#' Determines the best source to use:
#' - If input is already GeoParquet: use directly
#' - If input is GDB/GPKG and large: convert to GeoParquet (or use existing)
#' - If input is small: use directly
#'
#' @param input_path Path to input file (GDB, GPKG, or GeoParquet)
#' @param use_geoparquet "auto", TRUE, or FALSE
#' @param output_pbf Path to output PBF (for determining GeoParquet location)
#' @param verbose Print progress messages
#' @return List with path, is_geoparquet flag, and needs_cleanup flag
#' @noRd
resolve_source <- function(input_path, use_geoparquet = "auto", output_pbf = NULL, verbose = TRUE) {
  
  # CASE 1: Input is already GeoParquet - use directly
  if (grepl("\\.(geoparquet|parquet)$", input_path, ignore.case = TRUE)) {
    if (verbose) {
      cli::cli_alert_success("Using provided GeoParquet: {.file {input_path}}")
    }
    return(list(
      path = input_path,
      is_geoparquet = TRUE,
      needs_cleanup = FALSE
    ))
  }
  
  # Input is GDB or GPKG - determine if we should convert
  
  # Calculate source file size
  if (dir.exists(input_path)) {
    # FileGDB is a directory
    source_size <- sum(file.info(list.files(input_path, full.names = TRUE, recursive = TRUE))$size, na.rm = TRUE)
  } else {
    # GPKG or other single file
    source_size <- file.size(input_path)
  }
  
  # Determine GeoParquet output path
  if (is.null(output_pbf)) {
    # Default: same location as input
    geoparquet_path <- sub("\\.(gdb|gpkg)$", ".geoparquet", input_path, ignore.case = TRUE)
  } else {
    # Place next to output PBF
    base_name <- tools::file_path_sans_ext(basename(input_path))
    geoparquet_path <- file.path(dirname(output_pbf), paste0(base_name, ".geoparquet"))
  }
  
  # Determine if we should use GeoParquet
  if (use_geoparquet == "auto") {
    # Auto-enable for files > 50MB
    use_geoparquet <- source_size > 50 * 1024 * 1024
  }
  
  if (!use_geoparquet) {
    # Use source directly
    return(list(
      path = input_path,
      is_geoparquet = FALSE,
      needs_cleanup = FALSE
    ))
  }
  
  # CASE 2: Convert or use existing GeoParquet
  ensure_geoparquet(input_path, geoparquet_path, verbose)
  
  list(
    path = geoparquet_path,
    is_geoparquet = TRUE,
    needs_cleanup = FALSE
  )
}
