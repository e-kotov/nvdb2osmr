#' Get NVDB Column Name Mappings
#'
#' Returns a named list mapping short ASCII column names (as they appear in
#' GDB/GeoParquet files) to long descriptive Swedish names. This is useful for
#' documentation and understanding what each column represents.
#'
#' The column mappings are stored in `inst/extdata/column_mappings.yaml` and
#' intentionally contain non-ASCII characters (Swedish åäö) for human reference.
#' Package code should always use the short ASCII names.
#'
#' @return A named list where names are short column names and values are long
#'   descriptive Swedish names.
#' @export
#'
#' @examples
#' \dontrun{
#' mappings <- get_column_mappings()
#' mappings$Vagnr_10370  # "Driftbidrag statligt/Vägnr"
#'
#' # Get all columns related to roads (väg)
#' mappings[grepl("vag", names(mappings), ignore.case = TRUE)]
#' }
get_column_mappings <- function() {
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required but not installed.")
  }
  yaml_path <- system.file("extdata", "column_mappings.yaml", package = "nvdb2osmr")
  if (yaml_path == "") {
    stop("Column mappings file not found. Package installation may be incomplete.")
  }
  yaml::read_yaml(yaml_path)$columns
}


#' Get Long Column Name
#'
#' Look up the descriptive (long) name for a given short ASCII column name.
#'
#' @param short_name Character string with the short column name (e.g., "Vagnr_10370")
#' @return Character string with the long descriptive name, or the input if not found
#' @export
#'
#' @examples
#' \dontrun{
#' get_long_name("Vagnr_10370")  # "Driftbidrag statligt/Vägnr"
#' get_long_name("Klass_181")    # "Funktionell vägklass/Klass"
#' }
get_long_name <- function(short_name) {
  mappings <- get_column_mappings()
  if (short_name %in% names(mappings)) {
    mappings[[short_name]]
  } else {
    short_name
  }
}


#' List Available NVDB Columns
#'
#' Returns a data frame with all available column mappings, useful for
#' exploring the dataset structure.
#'
#' @param pattern Optional regex pattern to filter column names
#' @return A data frame with columns `short_name` and `long_name`
#' @export
#'
#' @examples
#' \dontrun{
#' # All columns
#' list_columns()
#'
#' # Columns related to speed (hastighet)
#' list_columns("hastighet")
#'
#' # Columns with "vag" in the name
#' list_columns("vag")
#' }
list_columns <- function(pattern = NULL) {
  mappings <- get_column_mappings()
  result <- data.frame(
    short_name = names(mappings),
    long_name = unlist(mappings, use.names = FALSE),
    stringsAsFactors = FALSE
  )

  if (!is.null(pattern)) {
    keep <- grepl(pattern, result$short_name, ignore.case = TRUE) |
      grepl(pattern, result$long_name, ignore.case = TRUE)
    result <- result[keep, ]
  }

  rownames(result) <- NULL
  result
}
