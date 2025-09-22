#!/usr/bin/env Rscript

# Example usage and benchmarking script for the bloomjoin package.
#
# The script demonstrates:
#   * Building sample data frames with controllable overlap
#   * Verifying that bloom_join() returns the same rows as dplyr joins
#   * Benchmarking execution time and memory allocations with bench::mark()
#
# Run from the package root with:
#   Rscript inst/scripts/usage-and-benchmark.R

suppressPackageStartupMessages({
  library(bloomjoin)
  library(dplyr)
  library(tibble)
  library(bench)
})

# Generate reproducible test data with a tunable overlap percentage.
generate_data <- function(n_left, n_right, overlap_pct = 0.1, seed = 42) {
  set.seed(seed)

  n_overlap <- round(min(n_left, n_right) * overlap_pct)
  key_pool <- sample(seq_len(n_left + n_right), n_left + n_right - n_overlap)

  left_ids <- key_pool[seq_len(n_left)]
  right_ids <- c(
    sample(left_ids, n_overlap),
    key_pool[(n_left + 1):(n_left + n_right - n_overlap)]
  )

  left <- tibble(
    id = left_ids,
    value_left = rnorm(n_left),
    group_left = sample(LETTERS[1:4], n_left, replace = TRUE)
  )

  right <- tibble(
    id = sample(right_ids),
    value_right = rnorm(n_right),
    flag_right = sample(c(TRUE, FALSE), n_right, replace = TRUE)
  )

  list(left = left, right = right)
}

# Validate correctness against dplyr joins.
validate_correctness <- function(data) {
  bloom <- bloom_join(data$left, data$right, by = "id") %>% arrange(id)
  reference <- inner_join(data$left, data$right, by = "id") %>% arrange(id)

  if (!identical(bloom, reference)) {
    stop("bloom_join results diverge from dplyr::inner_join")
  }

  message("✅ bloom_join matches dplyr::inner_join for the sample data")
}

# Benchmark time and memory using bench::mark()
run_benchmark <- function(data, iterations = 5) {
  bench::mark(
    bloom_join = bloom_join(data$left, data$right, by = "id"),
    dplyr_join = inner_join(data$left, data$right, by = "id"),
    iterations = iterations,
    check = FALSE
  )
}

main <- function() {
  message("=== bloomjoin usage and benchmark demo ===")
  sample_data <- generate_data(75000, 2500, overlap_pct = 0.05)

  message("\nValidating correctness...")
  validate_correctness(sample_data)

  message("\nBenchmarking (time & memory)...")
  bench_results <- run_benchmark(sample_data, iterations = 5)
  print(bench_results[, c("expression", "median", "itr/sec", "mem_alloc", "gc")])

  ratio <- bench_results$median[bench_results$expression == "dplyr_join"] /
    bench_results$median[bench_results$expression == "bloom_join"]
  message(sprintf("\nRelative speed-up (dplyr / bloom): %.2fx", ratio))
}

if (sys.nframe() == 0) {
  main()
}

