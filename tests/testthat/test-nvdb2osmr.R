test_that("Package loads correctly", {
  expect_true(requireNamespace("nvdb2osmr", quietly = TRUE))
})

test_that("Core functions exist", {
  expect_true(exists("process_nvdb_fast", where = asNamespace("nvdb2osmr")))
  expect_true(exists("process_nvdb_wkb", where = asNamespace("nvdb2osmr")))
  expect_true(exists("nvdb_to_pbf", where = asNamespace("nvdb2osmr")))
})

test_that("Input validation works", {
  # Test file not found
  expect_error(
    process_nvdb_fast("nonexistent.gdb", "/tmp/test.osm.pbf", "2480"),
    "Input file not found"
  )
  
  # Split-like calls require global dictionary path
  skip_if_not(file.exists("testdata/umea.gdb"), "Test data not available")
  expect_error(
    process_nvdb_fast(
      "testdata/umea.gdb",
      tempfile(fileext = ".osm.pbf"),
      municipality_code = "2480"
    ),
    "global_node_dict_path is required"
  )
  
  # Test invalid simplify_method
  expect_error(
    process_nvdb_fast(
      "testdata/umea.gdb",
      tempfile(fileext = ".osm.pbf"),
      simplify_method = "invalid"
    ),
    "must be one of"
  )
})

test_that("Single-file conversion works through nvdb_to_pbf", {
  skip_if_not(file.exists("testdata/umea.gdb"), "Test data not available")
  skip_if_not_installed("mirai")
  
  temp_file <- tempfile(fileext = ".osm.pbf")
  on.exit(unlink(temp_file), add = TRUE)
  
  result <- nvdb_to_pbf(
    input_path = "testdata/umea.gdb",
    output_pbf = temp_file,
    split_by = "none",
    global_node_prepass = "auto",
    use_geoparquet = "auto",
    max_retries = 1,
    duckdb_memory_limit_gb = 2,
    duckdb_threads = 1
  )
  
  expect_true(file.exists(temp_file))
  expect_gt(file.size(temp_file), 0)
  expect_equal(result, temp_file)
})
