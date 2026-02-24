test_that("nvdb_to_pbf rejects split mode with global_node_prepass='off' (municipality)", {
  expect_error(
    nvdb_to_pbf(
      input_path = "nonexistent.gdb",
      output_pbf = tempfile(fileext = ".osm.pbf"),
      split_by = "municipality",
      global_node_prepass = "off"
    ),
    "global_node_prepass='off' is not allowed",
    fixed = TRUE
  )
})

test_that("nvdb_to_pbf rejects split mode with global_node_prepass='off' (county)", {
  expect_error(
    nvdb_to_pbf(
      input_path = "nonexistent.gdb",
      output_pbf = tempfile(fileext = ".osm.pbf"),
      split_by = "county",
      global_node_prepass = "off"
    ),
    "global_node_prepass='off' is not allowed",
    fixed = TRUE
  )
})

test_that("nvdb_to_pbf warns and ignores prepass setting for split_by='none'", {
  skip_if_not(file.exists("testdata/umea.gdb"), "Test data not available")
  skip_if_not_installed("mirai")

  out_file <- tempfile(fileext = ".osm.pbf")
  on.exit(unlink(out_file), add = TRUE)

  expect_warning(
    result <- nvdb_to_pbf(
      input_path = "testdata/umea.gdb",
      output_pbf = out_file,
      split_by = "none",
      global_node_prepass = "on",
      use_geoparquet = "auto",
      max_retries = 1,
      duckdb_memory_limit_gb = 2,
      duckdb_threads = 1
    ),
    "ignored when split_by='none'",
    fixed = FALSE
  )

  expect_true(file.exists(out_file))
  expect_gt(file.size(out_file), 0)
  expect_equal(result, out_file)
})

test_that("process_nvdb_fast requires global dictionary for split-like calls", {
  skip_if_not(file.exists("testdata/umea.gdb"), "Test data not available")

  expect_error(
    process_nvdb_fast(
      gdb_path = "testdata/umea.gdb",
      output_pbf = tempfile(fileext = ".osm.pbf"),
      municipality_code = "2480",
      global_node_dict_path = NULL
    ),
    "global_node_dict_path is required",
    fixed = TRUE
  )
})
