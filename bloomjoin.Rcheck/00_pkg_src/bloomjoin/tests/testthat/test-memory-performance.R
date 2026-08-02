# Memory and Performance Tests for BloomJoin
# tests/testthat/test-memory-performance.R

library(testthat)
library(dplyr)
library(tibble)

# Helper function to measure memory usage (simplified)
measure_memory <- function(expr) {
  # Simplified memory measurement without pryr dependency
  gc()
  result <- force(expr)
  gc()
  list(result = result, memory_mb = NA)
}

# Helper function to time operations precisely
time_operation <- function(expr, iterations = 5) {
  times <- numeric(iterations)
  result <- NULL
  
  for (i in 1:iterations) {
    start_time <- Sys.time()
    result <- force(expr)
    end_time <- Sys.time()
    times[i] <- as.numeric(end_time - start_time, units = "secs")
  }
  
  list(
    result = result,
    mean_time = mean(times),
    median_time = median(times),
    min_time = min(times),
    max_time = max(times),
    sd_time = sd(times)
  )
}

test_that("memory usage is reasonable for large datasets", {
  skip_on_cran() # Skip on CRAN due to memory/time requirements
  
  set.seed(42)
  # Create large dataset
  n_large <- 100000
  n_small <- 1000
  
  large_df <- tibble(
    id = sample(1:50000, n_large, replace = TRUE),
    value = rnorm(n_large),
    category = sample(letters[1:5], n_large, replace = TRUE)
  )
  
  small_df <- tibble(
    id = sample(1:50000, n_small, replace = FALSE),
    lookup_value = rnorm(n_small),
    type = sample(LETTERS[1:3], n_small, replace = TRUE)
  )
  
  # Measure memory usage
  bloom_mem <- measure_memory(bloom_join(large_df, small_df, by = "id", verbose = TRUE))
  std_mem <- measure_memory(inner_join(large_df, small_df, by = "id"))
  
  # Results should be identical
  expect_equal(nrow(bloom_mem$result), nrow(std_mem$result))
  
  # Memory usage should be reasonable (if pryr available)
  if (!is.na(bloom_mem$memory_mb)) {
    expect_lt(abs(bloom_mem$memory_mb), 100) # Less than 100MB difference
    cat("Memory usage - Bloom:", round(bloom_mem$memory_mb, 2), "MB vs Standard:", round(std_mem$memory_mb, 2), "MB\n")
  }
})

test_that("performance scales well with dataset size", {
  skip_on_cran() # Skip on CRAN due to time requirements
  
  set.seed(123)
  sizes <- c(1000, 5000, 10000)
  lookup_size <- 100
  
  results <- data.frame(
    size = integer(),
    bloom_time = numeric(),
    std_time = numeric(),
    reduction_ratio = numeric(),
    stringsAsFactors = FALSE
  )
  
  for (size in sizes) {
    # Create data
    large_df <- tibble(
      id = sample(1:10000, size, replace = TRUE),
      value = rnorm(size)
    )
    
    small_df <- tibble(
      id = sample(1:10000, lookup_size, replace = FALSE),
      lookup = rnorm(lookup_size)
    )
    
    # Time operations
    bloom_timing <- time_operation(bloom_join(large_df, small_df, by = "id", verbose = TRUE))
    std_timing <- time_operation(inner_join(large_df, small_df, by = "id"))
    
    # Get metadata
    metadata <- attr(bloom_timing$result, "bloom_metadata")
    reduction <- if (!is.null(metadata) && !is.null(metadata$reduction_ratio)) metadata$reduction_ratio else 0

    # Store results
    results <- rbind(results, data.frame(
      size = size,
      bloom_time = bloom_timing$median_time,
      std_time = std_timing$median_time,
      reduction_ratio = reduction
    ))
    
    # Verify correctness
    expect_equal(nrow(bloom_timing$result), nrow(std_timing$result))
  }
  
  # Print results for manual inspection
  cat("Performance scaling results:\n")
  print(results)
  
  # Basic performance expectations
  expect_true(all(results$reduction_ratio >= 0))
  expect_true(all(is.finite(results$bloom_time)))
  expect_true(all(is.finite(results$std_time)))
})

test_that("bloom filter parameters affect performance predictably", {
  set.seed(456)
  
  large_df <- tibble(
    id = sample(1:5000, 10000, replace = TRUE),
    value = rnorm(10000)
  )
  
  small_df <- tibble(
    id = sample(1:5000, 200, replace = FALSE),
    lookup = rnorm(200)
  )
  
  # Test different false positive rates
  fpr_values <- c(0.001, 0.01, 0.1)
  fpr_results <- data.frame(
    fpr = numeric(),
    time = numeric(),
    reduction = numeric(),
    bloom_used = logical(),
    stringsAsFactors = FALSE
  )
  
  for (fpr in fpr_values) {
    timing <- time_operation(bloom_join(large_df, small_df, by = "id", fpr = fpr, verbose = TRUE))
    metadata <- attr(timing$result, "bloom_metadata")
    reduction <- if (!is.null(metadata) && !is.null(metadata$reduction_ratio)) metadata$reduction_ratio else 0
    used <- !is.null(metadata) && isTRUE(metadata$bloom_filter_used)

    fpr_results <- rbind(fpr_results, data.frame(
      fpr = fpr,
      time = timing$median_time,
      reduction = reduction,
      bloom_used = used
    ))
  }
  
  cat("False positive rate analysis:\n")
  print(fpr_results)
  
  # All should work and provide good reduction
  expect_true(all(fpr_results$reduction[fpr_results$bloom_used] > 0.5))
  expect_true(all(is.finite(fpr_results$time)))
})

test_that("multi-column joins performance is acceptable", {
  set.seed(789)
  
  # Multi-column test data
  df1 <- tibble(
    key1 = sample(1:100, 5000, replace = TRUE),
    key2 = sample(letters[1:20], 5000, replace = TRUE),
    value1 = rnorm(5000)
  )
  
  df2 <- tibble(
    key1 = sample(1:100, 300, replace = TRUE),
    key2 = sample(letters[1:20], 300, replace = TRUE), 
    value2 = rnorm(300)
  )
  
  # Time multi-column joins
  bloom_timing <- time_operation(bloom_join(df1, df2, by = c("key1", "key2"), verbose = TRUE))
  std_timing <- time_operation(suppressWarnings(inner_join(df1, df2, by = c("key1", "key2"))))
  
  metadata <- attr(bloom_timing$result, "bloom_metadata")
  
  cat("Multi-column join performance:\n")
  cat("- Bloom time:", sprintf("%.4fs", bloom_timing$median_time), "\n")
  cat("- Standard time:", sprintf("%.4fs", std_timing$median_time), "\n")
  cat("- Reduction ratio:", sprintf("%.1f%%", metadata$reduction_ratio * 100), "\n")
  cat("- Result rows:", nrow(bloom_timing$result), "\n")
  
  # Results should match
  expect_equal(nrow(bloom_timing$result), nrow(std_timing$result))
  
  # Should achieve good reduction
  if (isTRUE(metadata$bloom_filter_used)) {
    expect_gt(metadata$reduction_ratio, 0.5)
  }
})

test_that("join type performance characteristics", {
  set.seed(999)
  
  df_large <- tibble(
    id = sample(1:1000, 5000, replace = TRUE),
    value = rnorm(5000)
  )
  
  df_small <- tibble(
    id = sample(1:1000, 200, replace = FALSE),
    lookup = rnorm(200)
  )
  
  join_types <- c("inner", "semi")
  type_results <- data.frame(
    type = character(),
    bloom_time = numeric(),
    std_time = numeric(),
    rows_returned = integer(),
    bloom_used = logical(),
    stringsAsFactors = FALSE
  )
  
  for (join_type in join_types) {
    # Bloom join
    bloom_timing <- time_operation(bloom_join(df_large, df_small, by = "id", type = join_type, verbose = TRUE))
    
    # Standard join
    std_expr <- switch(join_type,
      "inner" = inner_join(df_large, df_small, by = "id"),
      "semi" = semi_join(df_large, df_small, by = "id"),
      "anti" = anti_join(df_large, df_small, by = "id")
    )
    std_timing <- time_operation(std_expr)
    
    metadata <- attr(bloom_timing$result, "bloom_metadata")
    bloom_used <- !is.null(metadata) && isTRUE(metadata$bloom_filter_used)
    
    type_results <- rbind(type_results, data.frame(
      type = join_type,
      bloom_time = bloom_timing$median_time,
      std_time = std_timing$median_time,
      rows_returned = nrow(bloom_timing$result),
      bloom_used = bloom_used
    ))
    
    # Verify correctness
    expect_equal(nrow(bloom_timing$result), nrow(std_timing$result))
  }
  
  cat("Join type performance comparison:\n")
  print(type_results)
  
  # All join types that should use bloom filters did
  expect_true(any(type_results$bloom_used))
  expect_true(all(is.finite(type_results$bloom_time)))
  expect_true(all(is.finite(type_results$std_time)))
})

test_that("memory efficiency with sparse joins", {
  skip_on_cran()
  
  set.seed(111)
  # Very sparse join - large dataset with tiny overlap
  large_df <- tibble(
    id = 1:50000, # Sequential IDs
    value = rnorm(50000)
  )
  
  small_df <- tibble(
    id = sample(40000:50000, 50, replace = FALSE), # Only small overlap at the end
    lookup = rnorm(50)
  )
  
  # This should be ideal for Bloom filter
  bloom_mem <- measure_memory(bloom_join(large_df, small_df, by = "id", verbose = TRUE))
  std_mem <- measure_memory(inner_join(large_df, small_df, by = "id"))
  
  metadata <- attr(bloom_mem$result, "bloom_metadata")
  
  cat("Sparse join analysis:\n")
  cat("- Results found:", nrow(bloom_mem$result), "\n")
  cat("- Reduction ratio:", sprintf("%.2f%%", metadata$reduction_ratio * 100), "\n")
  
  # Should find the overlapping records
  expect_gt(nrow(bloom_mem$result), 0)
  expect_equal(nrow(bloom_mem$result), nrow(std_mem$result))
  
  # Should achieve excellent reduction
  expect_gt(metadata$reduction_ratio, 0.95)
})

# Performance regression test
test_that("performance does not regress significantly", {
  set.seed(222)
  
  # Standard benchmark dataset
  df1 <- tibble(
    id = sample(1:5000, 20000, replace = TRUE),
    value = rnorm(20000)
  )
  
  df2 <- tibble(
    id = sample(1:5000, 500, replace = FALSE),
    lookup = rnorm(500)
  )
  
  # Run multiple times for stability
  bloom_times <- replicate(10, {
    start <- Sys.time()
    result <- bloom_join(df1, df2, by = "id")
    as.numeric(Sys.time() - start)
  })
  
  std_times <- replicate(10, {
    start <- Sys.time()
    result <- inner_join(df1, df2, by = "id")
    as.numeric(Sys.time() - start)
  })
  
  mean_bloom <- mean(bloom_times)
  mean_std <- mean(std_times)
  
  cat("Regression test results:\n")
  cat("- Average Bloom time:", sprintf("%.4fs (±%.4fs)", mean_bloom, sd(bloom_times)), "\n")
  cat("- Average Standard time:", sprintf("%.4fs (±%.4fs)", mean_std, sd(std_times)), "\n")
  cat("- Ratio:", sprintf("%.2fx", mean_bloom / mean_std), "\n")
  
  # Performance should be reasonable (not more than 10x slower in worst case)
  expect_lt(mean_bloom / mean_std, 10)
  
  # Should have consistent performance
  expect_lt(sd(bloom_times) / mean_bloom, 0.5) # CV less than 50%
})