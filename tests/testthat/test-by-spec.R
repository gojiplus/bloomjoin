## dplyr lets `by` mix named and unnamed elements in one vector: an unnamed
## element joins a column to itself, a named one maps names(by) in x to by in y.
##
## resolve_join_columns() entered its named branch when *any* element was named
## and then took names(by) wholesale, so every unnamed entry became "" and was
## looked up as a column name -- `by = c("k", a = "b")` died with
## "Join columns not found in x: " naming nothing at all.

n <- 3000L

test_that("`by` may mix named and unnamed elements, as in dplyr", {
  x <- data.frame(k = seq_len(n), a = seq_len(n), vx = seq_len(n))
  y <- data.frame(k = seq_len(n), b = seq_len(n), vy = seq_len(n))

  got <- suppressMessages(bloom_join(x, y, by = c("k", a = "b")))
  ref <- suppressMessages(dplyr::inner_join(x, y, by = c("k", a = "b")))
  expect_equal(as.data.frame(got), as.data.frame(ref), ignore_attr = TRUE)
})

test_that("the wholly named and wholly unnamed forms still agree with dplyr", {
  xa <- data.frame(a = seq_len(n), vx = seq_len(n))
  yb <- data.frame(b = seq_len(n), vy = seq_len(n))
  expect_equal(
    as.data.frame(suppressMessages(bloom_join(xa, yb, by = c(a = "b")))),
    as.data.frame(suppressMessages(dplyr::inner_join(xa, yb, by = c(a = "b")))),
    ignore_attr = TRUE
  )

  x <- data.frame(k = seq_len(n), vx = seq_len(n))
  y <- data.frame(k = seq_len(n), vy = seq_len(n))
  expect_equal(
    as.data.frame(suppressMessages(bloom_join(x, y, by = "k"))),
    as.data.frame(suppressMessages(dplyr::inner_join(x, y, by = "k"))),
    ignore_attr = TRUE
  )
})

test_that("a genuinely absent column is still reported by its own name", {
  x <- data.frame(k = seq_len(n), vx = seq_len(n))
  y <- data.frame(k = seq_len(n), vy = seq_len(n))
  expect_error(
    suppressMessages(bloom_join(x, y, by = c("k", "nope"))),
    "not found in x: nope"
  )
})
