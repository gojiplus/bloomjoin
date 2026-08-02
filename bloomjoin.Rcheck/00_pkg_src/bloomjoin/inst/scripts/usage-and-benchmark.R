#!/usr/bin/env Rscript
# Usage and benchmark script for bloomjoin
# Run: Rscript inst/scripts/usage-and-benchmark.R

suppressPackageStartupMessages({
  library(bloomjoin)
  library(dplyr)
  library(tibble)
  library(bench)
})

generate_data <- function(n_left, n_right, overlap_pct = 0.1, seed = 42) {
  set.seed(seed)
  n_overlap <- round(min(n_left, n_right) * overlap_pct)
  key_pool <- sample(seq_len(n_left + n_right), n_left + n_right - n_overlap)

  left_ids <- key_pool[seq_len(n_left)]
  right_ids <- c(
    sample(left_ids, n_overlap),
    key_pool[(n_left + 1):(n_left + n_right - n_overlap)]
  )

  list(
    left = tibble(id = left_ids, value_left = rnorm(n_left)),
    right = tibble(id = sample(right_ids), value_right = rnorm(n_right))
  )
}

validate_correctness <- function(data) {
  bloom <- bloom_join(data$left, data$right, by = "id") |> arrange(id)
  reference <- inner_join(data$left, data$right, by = "id") |> arrange(id)

  bloom_cmp <- as.data.frame(bloom)
  attr(bloom_cmp, "bloom_metadata") <- NULL
  ref_cmp <- as.data.frame(reference)

  if (!isTRUE(all.equal(bloom_cmp, ref_cmp, check.attributes = FALSE))) {
    stop("bloom_join results diverge from dplyr::inner_join")
  }
  message("Correctness: OK")
}

run_benchmark <- function(data, iterations = 5) {
  bench::mark(
    bloom = bloom_join(data$left, data$right, by = "id"),
    dplyr = inner_join(data$left, data$right, by = "id"),
    iterations = iterations,
    check = FALSE
  )
}

main <- function() {
  sample_data <- generate_data(75000, 2500, overlap_pct = 0.05)

  validate_correctness(sample_data)

  results <- run_benchmark(sample_data, iterations = 5)
  print(results[, c("expression", "median", "itr/sec", "mem_alloc")])

  ratio <- as.numeric(results$median[2]) / as.numeric(results$median[1])
  message(sprintf("Speed ratio (dplyr/bloom): %.2fx", ratio))
}

if (sys.nframe() == 0) {
  main()
}
