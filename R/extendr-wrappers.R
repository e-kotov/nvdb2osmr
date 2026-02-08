#' @docType _PACKAGE
#' @usage NULL
#' @useDynLib nvdb2osmr, .registration = TRUE
NULL

#' Process NVDB data to OSM PBF (WKB optimized)
#'
#' Optimized function using WKB geometries and direct R property columns.
#' This avoids JSON serialization overhead for significant speedup.
#'
#' @param wkb_geoms List of raw WKB byte vectors (one per geometry)
#' @param col_names Character vector of property column names
#' @param col_data List of vectors (one per column), each same length as wkb_geoms
#' @param output_path Path to write the output .osm.pbf file
#' @param simplify_method Simplification method: "refname" (default), "recursive", 
#'        "linear", "route", or "segment"
#' @param node_id_start Starting ID for nodes (default: 1)
#' @param way_id_start Starting ID for ways (default: 1)
#' @return TRUE on success
#'
#' @export
process_nvdb_wkb <- function(
    wkb_geoms,
    col_names,
    col_data,
    output_path,
    simplify_method = "refname",
    node_id_start = 1L,
    way_id_start = 1L
) {
    .Call(
        wrap__process_nvdb_wkb,
        wkb_geoms,
        col_names,
        col_data,
        output_path,
        simplify_method,
        as.integer(node_id_start),
        as.integer(way_id_start)
    )
}

# nolint start
# nocov start
.onLoad <- function(libname, pkgname) {
    so_file <- file.path(libname, pkgname, "libs", paste0(pkgname, ".so"))
    if (file.exists(so_file)) {
        library.dynam(pkgname, pkgname, libname)
    }
}

.onUnload <- function(libpath) {
    try(library.dynam.unload("nvdb2osmr", libpath), silent = TRUE)
}
# nocov end
# nolint end
