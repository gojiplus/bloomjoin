# Simplified test file for BloomJoin package
# tests/testthat/test-bloom_join.R

library(testthat)
library(dplyr)
library(tibble)

# Test that bloom_join produces same results as standard joins
test_that("bloom_join produces same results as standard joins", {
  # Create test data
  x <- tibble(
    id = 1:100,
    value_x = rnorm(100)
  )

  y <- tibble(
    id = c(50:150),
    value_y = rnorm(101)
  )

  # Test inner join
  inner_result_bloom <- bloom_join(x, y, by = "id", type = "inner")
  inner_result_std <- inner_join(x, y, by = "id")
  expect_equal(nrow(inner_result_bloom), nrow(inner_result_std))
  expect_equal(sort(inner_result_bloom$id), sort(inner_result_std$id))
  expect_equal(ncol(inner_result_bloom), ncol(inner_result_std))

  # Test left join - suppress the warning for the test
  suppressWarnings({
    left_result_bloom <- bloom_join(x, y, by = "id", type = "left")
  })
  left_result_std <- left_join(x, y, by = "id")
  expect_equal(nrow(left_result_bloom), nrow(left_result_std))
  expect_equal(sort(left_result_bloom$id), sort(left_result_std$id))
  expect_equal(ncol(left_result_bloom), ncol(left_result_std))

  # Test semi join
  semi_result_bloom <- bloom_join(x, y, by = "id", type = "semi")
  semi_result_std <- semi_join(x, y, by = "id")
  expect_equal(nrow(semi_result_bloom), nrow(semi_result_std))
  expect_equal(sort(semi_result_bloom$id), sort(semi_result_std$id))
  expect_equal(ncol(semi_result_bloom), ncol(semi_result_std))
})

# Test with multiple join columns
test_that("bloom_join works with multiple join columns", {
  # Create test data
  x <- tibble(
    id1 = rep(1:10, each = 10),
    id2 = rep(1:10, times = 10),
    value_x = rnorm(100)
  )

  y <- tibble(
    id1 = rep(5:15, each = 10),
    id2 = rep(1:10, times = 11),
    value_y = rnorm(110)
  )

  # Test inner join with multiple columns
  result_bloom <- bloom_join(x, y, by = c("id1", "id2"))
  result_std <- inner_join(x, y, by = c("id1", "id2"))

  expect_equal(nrow(result_bloom), nrow(result_std))
  expect_equal(sort(paste(result_bloom$id1, result_bloom$id2)),
               sort(paste(result_std$id1, result_std$id2)))
})

# Test with no overlap between datasets
test_that("bloom_join handles no overlap correctly", {
  # Create test data with no overlap
  x <- tibble(
    id = 1:100,
    value_x = rnorm(100)
  )

  y <- tibble(
    id = 101:200,
    value_y = rnorm(100)
  )

  # Test inner join
  inner_result_bloom <- bloom_join(x, y, by = "id")
  inner_result_std <- inner_join(x, y, by = "id")

  expect_equal(nrow(inner_result_bloom), 0)
  expect_equal(nrow(inner_result_std), 0)

  # Test left join - suppress warning
  suppressWarnings({
    left_result_bloom <- bloom_join(x, y, by = "id", type = "left")
  })
  left_result_std <- left_join(x, y, by = "id")

  expect_equal(nrow(left_result_bloom), nrow(x))
  expect_equal(nrow(left_result_std), nrow(x))
  expect_true(all(is.na(left_result_bloom$value_y)))
})

# Test with complete overlap
test_that("bloom_join handles complete overlap correctly", {
  # Create test data with complete overlap
  x <- tibble(
    id = 1:100,
    value_x = rnorm(100)
  )

  y <- tibble(
    id = 1:100,
    value_y = rnorm(100)
  )

  # Test inner join
  inner_result_bloom <- bloom_join(x, y, by = "id")
  inner_result_std <- inner_join(x, y, by = "id")

  expect_equal(nrow(inner_result_bloom), 100)
  expect_equal(nrow(inner_result_std), 100)
})

# Test with character join columns
test_that("bloom_join works with character columns", {
  # Create test data
  x <- tibble(
    id = letters[1:20],
    value_x = rnorm(20)
  )

  y <- tibble(
    id = letters[10:26],
    value_y = rnorm(17)
  )

  # Test join
  result_bloom <- bloom_join(x, y, by = "id")
  result_std <- inner_join(x, y, by = "id")

  expect_equal(nrow(result_bloom), nrow(result_std))
  expect_equal(sort(result_bloom$id), sort(result_std$id))
})

# Test with different column names
test_that("bloom_join handles different column names correctly", {
  # Create test data
  x <- tibble(
    id_x = 1:100,
    value_x = rnorm(100)
  )

  y <- tibble(
    id_y = 50:150,
    value_y = rnorm(101)
  )

  # Test join with named vector in by
  result_bloom <- bloom_join(x, y, by = c("id_x" = "id_y"))
  result_std <- inner_join(x, y, by = c("id_x" = "id_y"))

  expect_equal(nrow(result_bloom), nrow(result_std))
  expect_equal(sort(result_bloom$id_x), sort(result_std$id_x))
})

# Test error conditions
test_that("bloom_join throws appropriate errors", {
  # Test data
  x <- tibble(id = 1:10, value_x = rnorm(10))
  y <- tibble(id = 1:10, value_y = rnorm(10))

  # Test non-data frame input
  expect_error(bloom_join(list(1:10), y), "must be data frames")
  expect_error(bloom_join(x, list(1:10)), "must be data frames")

  # Test invalid join type
  expect_error(bloom_join(x, y, type = "invalid_type"), "Invalid join type")

  # Test missing join columns
  expect_error(bloom_join(x, y, by = c("non_existent")), "Join columns not found in x")
})

# Test with duplicated keys - suppress many-to-many warnings
test_that("bloom_join handles duplicated keys correctly", {
  # Create test data with duplicates in both tables
  x <- tibble(
    id = rep(1:10, each = 2),
    value_x = rnorm(20)
  )

  y <- tibble(
    id = rep(5:15, each = 3),
    value_y = rnorm(33)
  )

  # Cartesian product expected for duplicated keys
  suppressWarnings({
    result_bloom <- bloom_join(x, y, by = "id")
    result_std <- inner_join(x, y, by = "id")
  })

  expect_equal(nrow(result_bloom), nrow(result_std))
  # Should be 2 (duplicates in x) * 3 (duplicates in y) * 6 (overlapping keys) = 36
  expect_equal(nrow(result_bloom), 36)
})

test_that("prefilter side selection and metadata are exposed", {
  set.seed(123)
  x <- tibble(
    id = sample(1:10000, 6000, replace = TRUE),
    value_x = rnorm(6000)
  )

  y <- tibble(
    id = sample(1:500, 2000, replace = TRUE),
    value_y = rnorm(2000)
  )

  result <- bloom_join(x, y, by = "id", prefilter_side = "x")
  metadata <- attr(result, "bloom_metadata")
  expect_true(metadata$bloom_filter_used)
  expect_identical(metadata$chosen_prefilter_side, "x")
  expect_true(metadata$filtered_rows_x > 0)
})

test_that("bloom_params returns sensible defaults", {
  params <- bloom_params(1e5, p = 0.01)
  expect_equal(params$k, as.integer(round((params$m / 1e5) * log(2))), tolerance = 1)
  expect_true(params$m > 0)
})

# Test with NA values in join columns
test_that("bloom_join handles NA values in join columns", {
  # Create test data with NAs
  x <- tibble(
    id = c(1:8, NA, NA),
    value_x = rnorm(10)
  )

  y <- tibble(
    id = c(5:12, NA, NA),
    value_y = rnorm(10)
  )

  # Test join - suppress warnings about many-to-many joins with NA values
  suppressWarnings({
    result_bloom <- bloom_join(x, y, by = "id")
    result_std <- inner_join(x, y, by = "id")
  })

  expect_equal(nrow(result_bloom), nrow(result_std))
  expect_equal(sort(result_bloom$id, na.last = TRUE), sort(result_std$id, na.last = TRUE))
})

# Test with different false positive rates
test_that("bloom_join respects fpr parameter without changing results", {
  x <- tibble(
    id = sample(1:2000, 5000, replace = TRUE),
    value_x = rnorm(5000)
  )

  y <- tibble(
    id = sample(1:2000, 400, replace = FALSE),
    value_y = rnorm(400)
  )

  result_low <- bloom_join(x, y, by = "id", fpr = 0.001)
  result_high <- bloom_join(x, y, by = "id", fpr = 0.1)

  expect_equal(nrow(result_low), nrow(result_high))
  meta_low <- attr(result_low, "bloom_metadata")
  meta_high <- attr(result_high, "bloom_metadata")
  expect_equal(meta_low$fpr, 0.001, tolerance = 1e-12)
  expect_equal(meta_high$fpr, 0.1, tolerance = 1e-12)
})

# Test with extreme data sizes
test_that("bloom_join works with very small datasets", {
  # Create tiny test data
  x <- tibble(id = 1:3, value_x = rnorm(3))
  y <- tibble(id = 2:4, value_y = rnorm(3))

  result_bloom <- bloom_join(x, y, by = "id")
  result_std <- inner_join(x, y, by = "id")

  expect_equal(nrow(result_bloom), nrow(result_std))
  expect_equal(sort(result_bloom$id), sort(result_std$id))
})

# Test automatic determination of join columns
test_that("bloom_join automatically determines join columns correctly", {
  # Create test data with common columns
  x <- tibble(
    id = 1:100,
    common = rep(1:10, 10),
    value_x = rnorm(100)
  )

  y <- tibble(
    id = 50:150,
    # Make sure the recycled vector has right length
    common = rep(5:15, length.out = 101) %% 10 + 1,
    value_y = rnorm(101)
  )

  # Test with automatic column detection
  result_auto <- bloom_join(x, y)

  # Test with explicit column specification
  result_explicit <- bloom_join(x, y, by = c("id", "common"))

  # Should produce the same result
  expect_equal(nrow(result_auto), nrow(result_explicit))
  expect_equal(ncol(result_auto), ncol(result_explicit))

  # Should be equivalent to standard join
  result_std <- inner_join(x, y, by = c("id", "common"))
  expect_equal(nrow(result_auto), nrow(result_std))
})

# Test result structure - bloom_join should produce identical results to dplyr
test_that("bloom_join produces identical results to dplyr joins", {
  # Create test data
  x <- tibble(id = 1:100, value_x = rnorm(100))
  y <- tibble(id = 50:150, value_y = rnorm(101))

  bloom_result <- bloom_join(x, y)
  dplyr_result <- inner_join(x, y, by = "id")

  # Should produce identical results (except for class)
  # Remove the bloomjoin class for comparison
  bloom_result_no_class <- bloom_result
  class(bloom_result_no_class) <- class(dplyr_result)
  attr(bloom_result_no_class, "bloom_metadata") <- NULL
  expect_identical(bloom_result_no_class, dplyr_result)

  # Verify that bloomjoin class is present
  expect_true("bloomjoin" %in% class(bloom_result))
})
