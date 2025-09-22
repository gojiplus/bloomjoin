# Benchmarks for bloomjoin vs direct dplyr join across selectivity regimes
# Run with: source("inst/bench/bench-bloomjoin.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(bench)
  # library(bloomjoin)  # ensure devtools::load_all() or install first
})

# Helper to generate synthetic keys with a controlled overlap fraction
make_join_data <- function(n_x = 5e5,
                           n_y = 2e5,
                           overlap = 0.5,
                           dup_rate_y = 0.2,
                           seed = 123) {
  stopifnot(overlap >= 0, overlap <= 1)
  set.seed(seed)

  # Unique key universe sizes (keep simple but scalable)
  u_y <- max(round(n_y * (1 - dup_rate_y)), 10L)
  u_x_match <- max(round(n_x * overlap), 0L)
  u_x_nomatch <- n_x - u_x_match

  # Universe for keys; disjoint pool for non-matching x keys
  key_pool_y <- seq_len(u_y)
  key_pool_x_nomatch <- (u_y + 1L):(u_y + 10L + n_x)  # plenty of disjoint keys

  # y: mix of duplicates and uniques
  y <- tibble(
    key = sample(key_pool_y, size = n_y, replace = TRUE),
    yv  = runif(n_y)
  )

  # x: some keys drawn from y's universe (to control overlap), rest disjoint
  x <- tibble(
    key = c(
      if (u_x_match > 0) sample(key_pool_y, size = u_x_match, replace = TRUE) else integer(0),
      if (u_x_nomatch > 0) sample(key_pool_x_nomatch, size = u_x_nomatch, replace = TRUE) else integer(0)
    ),
    xv  = runif(n_x)
  )

  list(x = x, y = y)
}

bench_bloomjoin <- function(n_x = 5e5,
                            n_y = 2e5,
                            overlaps = c(0.05, 0.25, 0.5, 0.75, 0.95),
                            reps = 3,
                            seed = 123,
                            show_params = TRUE,
                            p_target = 1e-2) {
  results <- list()

  # Determine bloom params for the *build* side (y) for context
  if (show_params) {
    n_distinct_y <- n_y # conservative; you can replace with n_distinct(y$key) at runtime
    bp <- tryCatch(bloom_params(n_distinct_y, p = p_target), error = function(e) NULL)
    if (!is.null(bp)) {
      message(
        sprintf("Bloom params for n≈%s: m=%s bits (%.3f MiB), k=%d, target p=%.3g, est p=%.3g",
                format(n_distinct_y, big.mark = ","),
                format(bp$m_bits, big.mark = ","),
                bp$bytes / 1024^2, bp$k, bp$p_target, bp$fpr_est)
      )
    }
  }

  # Does installed bloom_join accept `engine=`? (will be TRUE after you add it)
  has_engine <- "engine" %in% names(formals(bloomjoin::bloom_join))

  if (!has_engine) {
    message("bloom_join() does not yet expose engine=; skipping bloom engine benchmarks.")
  }

  for (ov in overlaps) {
    dat <- make_join_data(n_x = n_x, n_y = n_y, overlap = ov, seed = seed)

    # Warm-up to stabilize dispatch
    invisible(inner_join(dat$x, dat$y, by = "key"))
    invisible(bloomjoin::bloom_join(dat$x, dat$y, by = "key", type = "inner"))

    b <- bench::mark(
      dplyr_inner = inner_join(dat$x, dat$y, by = "key"),
      bloomjoin_prefilter = bloomjoin::bloom_join(dat$x, dat$y, by = "key", type = "inner"),
      # Once you implement the true Bloom engine, uncomment this line:
      # bloomjoin_bloom = bloomjoin::bloom_join(dat$x, dat$y, by = "key", type = "inner", engine = "bloom", fpr = p_target),
      iterations = reps,
      check = FALSE,
      memory = TRUE,
      time_unit = "ms"
    ) |>
      dplyr::mutate(overlap = ov, n_x = n_x, n_y = n_y)

    results[[length(results) + 1L]] <- b
  }

  out <- dplyr::bind_rows(results) |>
    dplyr::select(overlap, n_x, n_y, expression, median, mem_alloc, `itr/sec`)
  print(out)
  invisible(out)
}

# Example: keep defaults modest so it runs on a laptop
if (sys.nframe() == 0L) {
  bench_bloomjoin(n_x = 2e5, n_y = 1e5, overlaps = c(0.1, 0.5, 0.9), reps = 5)
}
