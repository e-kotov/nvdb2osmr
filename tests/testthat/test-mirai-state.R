test_that("resolve_mirai_state returns sequential fallback state when unconfigured", {
  state <- nvdb2osmr:::resolve_mirai_state(
    daemons_configured = FALSE,
    info = c(connections = 4)
  )

  expect_false(state$daemons_configured)
  expect_identical(state$n_workers, 0L)
})

test_that("resolve_mirai_state reads active worker count from connections", {
  state <- nvdb2osmr:::resolve_mirai_state(
    daemons_configured = TRUE,
    info = c(connections = 2, cumulative = 2, awaiting = 0, executing = 0, completed = 0)
  )

  expect_true(state$daemons_configured)
  expect_identical(state$n_workers, 2L)
})

test_that("resolve_mirai_state handles sync mode", {
  state <- nvdb2osmr:::resolve_mirai_state(
    daemons_configured = TRUE,
    info = c(connections = 0, cumulative = 1, awaiting = 0, executing = 0, completed = 1)
  )

  expect_true(state$daemons_configured)
  expect_identical(state$n_workers, 0L)
})

test_that("resolve_mirai_state safely handles malformed info", {
  malformed_cases <- list(
    NULL,
    list(),
    list(connections = "2"),
    c(cumulative = 3),
    c(connections = NA_real_),
    c(connections = -1)
  )

  states <- lapply(malformed_cases, function(x) {
    nvdb2osmr:::resolve_mirai_state(
      daemons_configured = TRUE,
      info = x
    )
  })

  expect_true(all(vapply(states, `[[`, logical(1), "daemons_configured")))
  expect_true(all(vapply(states, `[[`, integer(1), "n_workers") == 0L))
})
