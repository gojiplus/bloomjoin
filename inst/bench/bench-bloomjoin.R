# Benchmarks for bloomjoin vs dplyr: speed and memory
# Run: source("inst/bench/bench-bloomjoin.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(bench)
})

make_join_data <- function(n_x, n_y, overlap, seed = 123) {

  set.seed(seed)
  y_keys <- seq_len(n_y)
  n_match <- round(n_x * overlap)
  x_matching <- if (n_match > 0) sample(y_keys, min(n_match, n_y), replace = n_match > n_y) else integer(0)
  x_nonmatching <- seq(n_y + 1, n_y + n_x - length(x_matching))

  list(
    x = tibble(key = sample(c(x_matching, x_nonmatching)), xv = runif(n_x)),
    y = tibble(key = y_keys, yv = runif(n_y))
  )
}

bench_scenario <- function(n_x, n_y, overlap, reps = 5) {
  dat <- make_join_data(n_x, n_y, overlap)

  # Warm up

  invisible(inner_join(dat$x, dat$y, by = "key"))
  invisible(bloomjoin::bloom_join(dat$x, dat$y, by = "key"))

  result <- bench::mark(
    bloom = bloomjoin::bloom_join(dat$x, dat$y, by = "key"),
    dplyr = inner_join(dat$x, dat$y, by = "key"),
    iterations = reps,
    check = FALSE,
    memory = TRUE
  )

  meta <- attr(bloomjoin::bloom_join(dat$x, dat$y, by = "key"), "bloom_metadata")

  data.frame(
    n_x = n_x,
    n_y = n_y,
    overlap = overlap,
    bloom_ms = round(as.numeric(result$median[1]) * 1000, 1),
    dplyr_ms = round(as.numeric(result$median[2]) * 1000, 1),
    speed_ratio = round(as.numeric(result$median[2]) / as.numeric(result$median[1]), 2),
    bloom_mb = round(as.numeric(result$mem_alloc[1]) / 1024^2, 1),
    dplyr_mb = round(as.numeric(result$mem_alloc[2]) / 1024^2, 1),
    mem_ratio = round(as.numeric(result$mem_alloc[2]) / as.numeric(result$mem_alloc[1]), 2),
    reduction = round(meta$reduction_ratio, 2)
  )
}

bench_bloomjoin <- function(scenarios = NULL, reps = 5) {
  if (is.null(scenarios)) {
    scenarios <- list(
      c(1e6, 1e4, 0.01),
      c(1e6, 1e4, 0.05),
      c(5e5, 5e3, 0.02),
      c(5e5, 5e3, 0.10),
      c(2e5, 2e4, 0.05),
      c(2e5, 2e4, 0.25),
      c(1e5, 1e5, 0.10),
      c(1e5, 1e5, 0.50)
    )
  }

  results <- do.call(rbind, lapply(scenarios, function(s) {
    bench_scenario(s[1], s[2], s[3], reps)
  }))

  print(results)
  invisible(results)
}

if (sys.nframe() == 0L) {
  bench_bloomjoin(reps = 3)
}
