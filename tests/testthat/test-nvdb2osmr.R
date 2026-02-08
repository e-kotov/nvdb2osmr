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
  
  # Test invalid municipality code (use a file that exists)
  skip_if_not(file.exists("testdata/umea.gdb"), "Test data not available")
  expect_error(
    process_nvdb_fast("testdata/umea.gdb", "/tmp/test.osm.pbf", "invalid"),
    "must be numeric"
  )
  
  # Test invalid simplify_method
  expect_error(
    process_nvdb_fast("testdata/umea.gdb", "/tmp/test.osm.pbf", "2480", simplify_method = "invalid"),
    "must be one of"
  )
})

test_that("Single municipality conversion works", {
  skip_if_not(file.exists("testdata/umea.gdb"), "Test data not available")
  
  temp_file <- tempfile(fileext = ".osm.pbf")
  on.exit(unlink(temp_file), add = TRUE)
  
  result <- process_nvdb_fast(
    gdb_path = "testdata/umea.gdb",
    output_pbf = temp_file,
    municipality_code = "2480",
    verbose = FALSE
  )
  
  expect_true(file.exists(temp_file))
  expect_gt(file.size(temp_file), 0)
  expect_equal(result, temp_file)
})
