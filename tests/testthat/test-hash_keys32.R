library(testthat)

test_that("hash_keys32 handles diverse column types", {
  df <- data.frame(
    int_col = c(1L, NA_integer_),
    dbl_col = c(0, runif(1)),
    chr_col = c("a", NA_character_),
    fac_col = factor(c("x", NA), levels = c("x", "y")),
    lgl_col = c(TRUE, NA),
    date_col = as.Date(c("2020-01-01", NA)),
    time_col = as.POSIXct(c("2020-01-01 00:00:00", "2020-01-02 00:00:00"), tz = "UTC"),
    stringsAsFactors = FALSE
  )

  hashes <- hash_keys32(df, by = names(df))
  expect_type(hashes, "integer")
  expect_length(hashes, nrow(df))
})

test_that("hash_keys32 treats -0 and +0 as the same", {
  df <- data.frame(dbl = c(0, -0))
  hashes <- hash_keys32(df, by = "dbl")
  expect_identical(hashes[1], hashes[2])
})

test_that("hash_keys32 normalizes NA and NaN consistently", {
  df <- data.frame(dbl = c(NA_real_, NaN))
  hashes <- hash_keys32(df, by = "dbl")
  expect_identical(hashes[1], hashes[2])
})

test_that("hash_keys32 handles POSIXlt inputs", {
  times <- as.POSIXlt(c("2020-01-01 00:00:00", "2020-01-02 00:00:00"), tz = "UTC")
  df <- data.frame(ts = times)
  hashes_lt <- hash_keys32(df, by = "ts")
  df_ct <- data.frame(ts = as.POSIXct(times))
  hashes_ct <- hash_keys32(df_ct, by = "ts")
  expect_identical(hashes_lt, hashes_ct)
})

test_that("hash_keys32 cooperates with bit64 when available", {
  skip_if_not_installed("bit64")
  df <- data.frame(
    id = bit64::as.integer64(c("1", "9223372036854775807")),
    stringsAsFactors = FALSE
  )
  hashes <- hash_keys32(df, by = "id")
  expect_type(hashes, "integer")
  expect_length(hashes, nrow(df))
})
