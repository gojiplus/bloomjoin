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

  # Test invalid join type - suppress warning about non-standard join
  suppressWarnings({
    expect_error(bloom_join(x, y, type = "invalid_type"), "Unsupported join type")
  })

  # Test missing join columns
  expect_error(bloom_join(x, y, by = c("non_existent")), "Not all join columns found")
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

# Test with different bloom_size parameters
test_that("bloom_join works with custom bloom_size parameter", {
  # Create test data
  x <- tibble(
    id = 1:1000,
    value_x = rnorm(1000)
  )

  y <- tibble(
    id = 501:1500,
    value_y = rnorm(1000)
  )

  # Test with smaller bloom size
  result_small <- bloom_join(x, y, by = "id", bloom_size = 100)

  # Test with larger bloom size
  result_large <- bloom_join(x, y, by = "id", bloom_size = 10000)

  # Test with default bloom size
  result_default <- bloom_join(x, y, by = "id")

  # All should produce the same result
  expect_equal(nrow(result_small), nrow(result_default))
  expect_equal(nrow(result_large), nrow(result_default))
  expect_equal(sort(result_small$id), sort(result_default$id))
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

# Test class assignment
test_that("bloom_join adds bloomjoin class to result", {
  # Create test data
  x <- tibble(id = 1:100, value_x = rnorm(100))
  y <- tibble(id = 50:150, value_y = rnorm(101))

  result <- bloom_join(x, y)

  # Check class
  expect_true("bloomjoin" %in% class(result))
})
