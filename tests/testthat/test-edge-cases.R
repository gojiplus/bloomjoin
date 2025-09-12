# Comprehensive Edge Case Tests for BloomJoin
# tests/testthat/test-edge-cases.R

library(testthat)
library(dplyr)
library(tibble)

test_that("extreme data sizes work correctly", {
  # Very small datasets
  x_tiny <- tibble(id = 1:3, value = 1:3)
  y_tiny <- tibble(id = 2:4, lookup = 4:6)
  
  result_tiny <- bloom_join(x_tiny, y_tiny, by = "id")
  expected_tiny <- inner_join(x_tiny, y_tiny, by = "id")
  
  expect_equal(nrow(result_tiny), nrow(expected_tiny))
  expect_equal(result_tiny$id, expected_tiny$id)
  
  # Single row datasets  
  x_single <- tibble(id = 1, value = 10)
  y_single <- tibble(id = 1, lookup = 20)
  
  result_single <- bloom_join(x_single, y_single, by = "id")
  expect_equal(nrow(result_single), 1)
  expect_equal(result_single$value, 10)
  expect_equal(result_single$lookup, 20)
})

test_that("extreme selectivity scenarios", {
  set.seed(42)
  
  # No overlap at all
  x_no_overlap <- tibble(id = 1:1000, value = rnorm(1000))
  y_no_overlap <- tibble(id = 2001:3000, lookup = rnorm(1000))
  
  result_no_overlap <- bloom_join(x_no_overlap, y_no_overlap, by = "id")
  expected_no_overlap <- inner_join(x_no_overlap, y_no_overlap, by = "id")
  
  expect_equal(nrow(result_no_overlap), 0)
  expect_equal(nrow(expected_no_overlap), 0)
  
  # 100% overlap
  x_full_overlap <- tibble(id = 1:500, value = rnorm(500))
  y_full_overlap <- tibble(id = 1:500, lookup = rnorm(500))
  
  result_full_overlap <- bloom_join(x_full_overlap, y_full_overlap, by = "id")
  expected_full_overlap <- inner_join(x_full_overlap, y_full_overlap, by = "id")
  
  expect_equal(nrow(result_full_overlap), 500)
  expect_equal(nrow(result_full_overlap), nrow(expected_full_overlap))
  
  # Single key overlap
  x_single_overlap <- tibble(id = 1:1000, value = rnorm(1000))
  y_single_overlap <- tibble(id = 500, lookup = 999)
  
  result_single_overlap <- bloom_join(x_single_overlap, y_single_overlap, by = "id")
  expect_equal(nrow(result_single_overlap), 1)
  expect_equal(result_single_overlap$id, 500)
})

test_that("complex NA patterns are handled correctly", {
  # All NAs in one dataset
  x_all_na <- tibble(id = rep(NA_integer_, 100), value = 1:100)
  y_normal <- tibble(id = 1:50, lookup = 51:100)
  
  result_all_na <- bloom_join(x_all_na, y_normal, by = "id")
  expected_all_na <- inner_join(x_all_na, y_normal, by = "id")
  
  expect_equal(nrow(result_all_na), nrow(expected_all_na))
  
  # Mixed NA patterns
  x_mixed_na <- tibble(
    id = c(1, 2, NA, 4, NA, 6, 7, NA),
    value = 1:8
  )
  y_mixed_na <- tibble(
    id = c(NA, 2, 3, NA, 5, 6, NA),
    lookup = 10:16
  )
  
  result_mixed <- bloom_join(x_mixed_na, y_mixed_na, by = "id", type = "inner")
  expected_mixed <- inner_join(x_mixed_na, y_mixed_na, by = "id")
  
  # Should handle NA matching correctly
  expect_equal(nrow(result_mixed), nrow(expected_mixed))
  expect_equal(sum(is.na(result_mixed$id)), sum(is.na(expected_mixed$id)))
})

test_that("duplicate key handling is correct", {
  # Many-to-many relationships
  x_dups <- tibble(
    id = rep(1:3, each = 10),
    seq = 1:30,
    value = rnorm(30)
  )
  y_dups <- tibble(
    id = rep(2:4, each = 5), 
    seq = 1:15,
    lookup = rnorm(15)
  )
  
  suppressWarnings({
    result_dups <- bloom_join(x_dups, y_dups, by = "id")
    expected_dups <- inner_join(x_dups, y_dups, by = "id")
  })
  
  # Should produce Cartesian product for matching keys
  expect_equal(nrow(result_dups), nrow(expected_dups))
  
  # Check specific cases
  id2_matches_bloom <- result_dups[result_dups$id == 2, ]
  id2_matches_std <- expected_dups[expected_dups$id == 2, ]
  expect_equal(nrow(id2_matches_bloom), nrow(id2_matches_std))
  expect_equal(nrow(id2_matches_bloom), 10 * 5) # 10 from x, 5 from y
})

test_that("special data types work correctly", {
  # Character keys with special characters
  x_char <- tibble(
    id = c("hello world", "test@email.com", "file/path.txt", "unicode-ñäöü", "123-456-7890"),
    value = 1:5
  )
  y_char <- tibble(
    id = c("test@email.com", "unicode-ñäöü", "new-key"),
    lookup = 10:12
  )
  
  result_char <- bloom_join(x_char, y_char, by = "id")
  expected_char <- inner_join(x_char, y_char, by = "id")
  
  expect_equal(nrow(result_char), 2)
  expect_equal(nrow(result_char), nrow(expected_char))
  expect_true("test@email.com" %in% result_char$id)
  expect_true("unicode-ñäöü" %in% result_char$id)
  
  # Date keys
  dates1 <- seq(as.Date("2023-01-01"), as.Date("2023-01-10"), by = "day")
  dates2 <- seq(as.Date("2023-01-05"), as.Date("2023-01-15"), by = "day")
  
  x_date <- tibble(date_key = dates1, value = 1:10)
  y_date <- tibble(date_key = dates2, lookup = 20:30)
  
  result_date <- bloom_join(x_date, y_date, by = "date_key")
  expected_date <- inner_join(x_date, y_date, by = "date_key")
  
  expect_equal(nrow(result_date), nrow(expected_date))
  expect_equal(nrow(result_date), 6) # 5 days overlap
})

test_that("memory pressure scenarios", {
  skip_on_cran() # Skip on CRAN due to memory requirements
  
  # Wide datasets (many columns)
  set.seed(123)
  n_rows <- 5000
  n_cols <- 50
  
  # Create wide x dataset
  x_wide_data <- matrix(rnorm(n_rows * n_cols), nrow = n_rows)
  x_wide <- as_tibble(x_wide_data, .name_repair = ~ paste0("col_", 1:n_cols))
  x_wide$id <- sample(1:1000, n_rows, replace = TRUE)
  
  # Create y dataset
  y_wide <- tibble(
    id = sample(1:1000, 200, replace = FALSE),
    lookup_col = rnorm(200)
  )
  
  # Should handle wide datasets
  result_wide <- bloom_join(x_wide, y_wide, by = "id")
  expected_wide <- inner_join(x_wide, y_wide, by = "id")
  
  expect_equal(nrow(result_wide), nrow(expected_wide))
  expect_equal(ncol(result_wide) - ncol(expected_wide), 0) # Same number of columns after cleanup
})

test_that("concurrent key patterns", {
  # Keys that might cause hash collisions
  x_collision <- tibble(
    # These strings are designed to potentially cause hash collisions
    id = c("a", "aa", "aaa", "aaaa", "aaaaa", paste0(rep("b", 100), collapse = "")),
    value = 1:6
  )
  y_collision <- tibble(
    id = c("aa", "aaaa", paste0(rep("b", 100), collapse = ""), "new_key"),
    lookup = 10:13
  )
  
  result_collision <- bloom_join(x_collision, y_collision, by = "id")
  expected_collision <- inner_join(x_collision, y_collision, by = "id")
  
  expect_equal(nrow(result_collision), nrow(expected_collision))
  expect_equal(sort(result_collision$id), sort(expected_collision$id))
})

test_that("extreme false positive rates", {
  set.seed(456)
  x <- tibble(id = 1:1000, value = rnorm(1000))
  y <- tibble(id = sample(1:1000, 100, replace = FALSE), lookup = rnorm(100))
  
  # Very low FPR (should still work)
  result_low_fpr <- bloom_join(x, y, by = "id", false_positive_rate = 0.0001)
  expected <- inner_join(x, y, by = "id")
  
  expect_equal(nrow(result_low_fpr), nrow(expected))
  
  # Higher FPR (should still be correct, just less efficient)
  result_high_fpr <- bloom_join(x, y, by = "id", false_positive_rate = 0.5)
  
  expect_equal(nrow(result_high_fpr), nrow(expected))
  
  # Both should produce identical results
  expect_equal(nrow(result_low_fpr), nrow(result_high_fpr))
})

test_that("join column name conflicts", {
  # Same column names in both datasets
  x_conflict <- tibble(
    id = 1:100,
    value = rnorm(100),
    common_name = "x_data"
  )
  y_conflict <- tibble(
    id = 50:150,
    value = rnorm(101), # Same column name as x
    common_name = "y_data"  # Same column name as x
  )
  
  result_conflict <- bloom_join(x_conflict, y_conflict, by = "id")
  expected_conflict <- inner_join(x_conflict, y_conflict, by = "id")
  
  # Should handle column name suffixes correctly
  expect_equal(nrow(result_conflict), nrow(expected_conflict))
  expect_true("value.x" %in% names(result_conflict) || "value" %in% names(result_conflict))
  expect_true("value.y" %in% names(result_conflict) || length(grep("value", names(result_conflict))) >= 1)
})

test_that("all join types work with edge cases", {
  x_edge <- tibble(id = c(1, 2, 3, NA, 5), value = 1:5)
  y_edge <- tibble(id = c(2, 4, NA, 6), lookup = 10:13)
  
  join_types <- c("inner", "left", "semi", "anti")
  
  for (join_type in join_types) {
    suppressWarnings({
      result_bloom <- bloom_join(x_edge, y_edge, by = "id", type = join_type)
      
      expected <- switch(join_type,
        "inner" = inner_join(x_edge, y_edge, by = "id"),
        "left" = left_join(x_edge, y_edge, by = "id"), 
        "semi" = semi_join(x_edge, y_edge, by = "id"),
        "anti" = anti_join(x_edge, y_edge, by = "id")
      )
    })
    
    expect_equal(nrow(result_bloom), nrow(expected), 
                info = paste("Join type:", join_type))
    expect_equal(sum(is.na(result_bloom$id)), sum(is.na(expected$id)),
                info = paste("NA handling for join type:", join_type))
  }
})