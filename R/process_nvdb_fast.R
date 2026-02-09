#' Fast NVDB to PBF conversion using ported Rust algorithm (WKB optimized)
#' 
#' @param gdb_path Path to input file (GDB, GPKG, or GeoParquet)
#' @param output_pbf Output PBF file path
#' @param municipality_code 4-digit municipality code to process (e.g., '2480')
#' @param county_code 2-digit county code to process (e.g., '24'). 
#'        Used if municipality_code is NULL.
#' @param simplify_method Simplification method (default: "refname")
#' @param node_id_start Starting node ID for this chunk (default: 1)
#' @param way_id_start Starting way ID for this chunk (default: 1)
#' @param duckdb_memory_limit_gb Memory limit for DuckDB in GB (numeric). Default 4.
#' @param duckdb_threads Number of threads for DuckDB. Default 1.
#' @param verbose Print progress messages (default: TRUE)
#' @return Path to output PBF file (invisibly)
#' @export
process_nvdb_fast <- function(gdb_path, output_pbf, 
                               municipality_code = NULL,
                               county_code = NULL,
                               simplify_method = "refname",
                               node_id_start = 1L,
                               way_id_start = 1L,
                               duckdb_memory_limit_gb = 4,
                               duckdb_threads = 1,
                               verbose = TRUE) {
  
  # --- Input Validation ---
  if (!is.character(gdb_path) || length(gdb_path) != 1) {
    stop("gdb_path must be a single character string")
  }
  if (!file.exists(gdb_path)) {
    stop("Input file not found: ", gdb_path)
  }
  
  if (!is.character(output_pbf) || length(output_pbf) != 1) {
    stop("output_pbf must be a single character string")
  }
  output_dir <- dirname(output_pbf)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  valid_methods <- c("refname", "connected")
  if (!simplify_method %in% valid_methods) {
    stop("simplify_method must be one of: ", paste(valid_methods, collapse = ", "))
  }
  
  # Detect input format
  is_geoparquet <- grepl("\\.(geoparquet|parquet)$", gdb_path, ignore.case = TRUE)
  
  # Key columns needed for tag mapping
  needed_cols <- c(
    "Motorvag", "Motortrafikled", "Klass_181",
    "Slitl_152",
    "Hogst_36", "Hogst_46", "F_Hogst_24",
    "Korfa_524", "F_ForbjudenFardriktning",
    "Ident_191", "Konst_190", "Vagtr_474", "Farjeled",
    "Namn_130", "Vagnr_10370", "Evag_555",
    "Bredd_156",
    "Typ_369",
    "ROUTE_ID", "Driftbidrag statligt/Vägnr",
    "Kommu_141"
  )
  
  # Progress function
  msg <- function(...) if (verbose) cli::cli_inform(...)
  
  msg("Reading data from {basename(gdb_path)}...")
  con <- DBI::dbConnect(duckdb::duckdb(), read_only = TRUE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Set resource limits
  limit_str <- paste0(as.integer(duckdb_memory_limit_gb), "GB")
  DBI::dbExecute(con, glue::glue_sql("SET memory_limit = {limit_str};", .con = con))
  DBI::dbExecute(con, glue::glue_sql("SET threads = {duckdb_threads};", .con = con))
  
  # Install/load required extensions
  tryCatch({
    DBI::dbExecute(con, "LOAD spatial;")
  }, error = function(e) {
    tryCatch({
      DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")
    }, error = function(e2) {
      stop("Failed to load DuckDB 'spatial' extension: ", conditionMessage(e2))
    })
  })

  # Build the filter
  where_sql <- DBI::SQL("")
  if (!is.null(municipality_code)) {
    where_sql <- glue::glue_sql("WHERE Kommu_141 = {municipality_code}", .con = con)
  } else if (!is.null(county_code)) {
    where_sql <- glue::glue_sql("WHERE Kommu_141 LIKE {paste0(county_code, '%')}", .con = con)
  }
  
  # Build query based on input format
  if (is_geoparquet) {
    # GeoParquet: Get column list using read_parquet
    all_cols <- DBI::dbGetQuery(con, glue::glue_sql("SELECT * FROM read_parquet({gdb_path}) LIMIT 0", .con = con))
    available_cols <- names(all_cols)
    
    # Priority for geometry column names
    geom_col_in_parquet <- NULL
    if ("geometry" %in% available_cols) {
      geom_col_in_parquet <- "geometry"
    } else if ("geom_wkb" %in% available_cols) {
      geom_col_in_parquet <- "geom_wkb"
    } else {
      # Try to find any column that DuckDB thinks is geometry
      for (col in available_cols) {
        test <- tryCatch({
          DBI::dbGetQuery(con, glue::glue_sql("
            SELECT ST_GeometryType({`col`}) as gtype 
            FROM read_parquet({gdb_path}) 
            WHERE {`col`} IS NOT NULL 
            LIMIT 1
          ", .con = con))
        }, error = function(e) NULL)
        if (!is.null(test) && nrow(test) > 0 && !is.na(test$gtype[1])) {
          geom_col_in_parquet <- col
          break
        }
      }
    }
    
    if (is.null(geom_col_in_parquet)) {
      stop("Could not detect geometry column in Parquet file: ", gdb_path)
    }

    # RUNTIME TYPE CHECK:
    # Check how DuckDB sees this column. This avoids Binder Errors by 
    # ensuring we only use ST_AsWKB if the column is actually interpreted as a GEOMETRY.
    type_info <- DBI::dbGetQuery(con, glue::glue_sql("DESCRIBE SELECT {`geom_col_in_parquet`} FROM read_parquet({gdb_path})", .con = con))
    actual_type <- toupper(type_info$column_type[1])
    
    geom_sql <- if (actual_type == "GEOMETRY") {
      # DuckDB automatically interpreted it as geometry, we must call ST_AsWKB
      glue::glue_sql("ST_AsWKB({`geom_col_in_parquet`})", .con = con)
    } else {
      # It's a BLOB (WKB), read directly
      glue::glue_sql("{`geom_col_in_parquet`}", .con = con)
    }
    
    select_cols <- intersect(needed_cols, available_cols)
    # Ensure we don't include the geometry column in properties
    select_cols <- setdiff(select_cols, geom_col_in_parquet)
    
    query <- glue::glue_sql("
      SELECT 
        {geom_sql} as wkb,
        {`select_cols`*}
      FROM read_parquet({gdb_path})
      {where_sql}
    ", .con = con)
    
  } else {
    # GDB or GPKG: need to detect geometry column and transform
    all_cols <- DBI::dbGetQuery(con, glue::glue_sql("
      SELECT * FROM ST_Read({gdb_path}) LIMIT 0
    ", .con = con))
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
          DBI::dbGetQuery(con, glue::glue_sql("
            SELECT ST_GeometryType({`col`}) as gtype 
            FROM ST_Read({gdb_path}) 
            WHERE {`col`} IS NOT NULL 
            LIMIT 1
          ", .con = con))
        }, error = function(e) NULL)
        if (!is.null(test) && nrow(test) > 0 && !is.na(test$gtype[1])) {
          geom_col <- col
          break
        }
      }
    }
    
    if (is.null(geom_col)) {
      stop("Could not detect geometry column in file: ", gdb_path)
    }
    
    select_cols <- intersect(needed_cols, available_cols)
    
    # Transform from SWEREF99 TM to WGS84
    sweref99_tm <- "+proj=utm +zone=33 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    wgs84 <- "+proj=longlat +datum=WGS84 +no_defs"
    
    query <- glue::glue_sql("
      SELECT 
        ST_AsWKB(ST_Transform(ST_Force2D({`geom_col`}), {sweref99_tm}, {wgs84})) as wkb,
        {`select_cols`*}
      FROM ST_Read({gdb_path}) 
      {where_sql}
    ", .con = con)
  }
  
  df <- DBI::dbGetQuery(con, query)
  msg("Read {nrow(df)} segments")
  
  if (nrow(df) == 0) {
    stop("No data found for area filter")
  }
  
  # WKB is already in raw bytes from DuckDB
  msg("Preparing WKB data for Rust...")
  wkb_list <- as.list(df$wkb)
  
  # Prepare properties as a named list of column vectors
  msg("Preparing properties...")
  prop_cols <- setdiff(names(df), "wkb")
  
  # Create a list of properties - each element is a column vector
  properties <- lapply(prop_cols, function(col) {
    vals <- df[[col]]
    if (is.character(vals) || is.factor(vals)) {
      as.character(vals)
    } else if (is.numeric(vals)) {
      vals
    } else if (is.logical(vals)) {
      vals
    } else {
      as.character(vals)
    }
  })
  names(properties) <- prop_cols
  
  msg("Processing {nrow(df)} segments in Rust...")
  result <- process_nvdb_wkb(
    wkb_geoms = wkb_list,
    col_names = prop_cols,
    col_data = properties,
    output_path = output_pbf,
    simplify_method = simplify_method,
    node_id_start = as.integer(node_id_start),
    way_id_start = as.integer(way_id_start)
  )
  
  if (!result) {
    stop("Rust processing failed")
  }
  
  # Get output stats if possible
  if (file.exists(output_pbf)) {
    size_mb <- file.size(output_pbf) / 1e6
    msg("Done! Output: {output_pbf} ({round(size_mb, 2)} MB)")
  } else {
    msg("Done!")
  }
  
  invisible(output_pbf)
}