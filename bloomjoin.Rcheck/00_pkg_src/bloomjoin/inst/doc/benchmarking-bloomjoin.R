## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup, message = FALSE---------------------------------------------------
library(bloomjoin)
library(dplyr)
library(bench)
library(knitr)

## ----helpers, include = FALSE-------------------------------------------------
generate_data <- function(n_x, n_y, overlap, seed = 123) {
  set.seed(seed)
  y_keys <- seq_len(n_y)
  n_match <- round(n_x * overlap)
  x_matching <- if (n_match > 0 && n_match <= n_y) {
    sample(y_keys, n_match)
  } else if (n_match > n_y) {
    sample(y_keys, n_match, replace = TRUE)
  } else {
    integer(0)
  }
  x_nonmatching <- seq(n_y + 1, n_y + n_x - length(x_matching))

  list(
    x = data.frame(id = sample(c(x_matching, x_nonmatching)), xv = rnorm(n_x)),
    y = data.frame(id = y_keys, yv = rnorm(n_y))
  )
}

run_bench <- function(n_x, n_y, overlap, reps = 3) {
  dat <- generate_data(n_x, n_y, overlap)

  # Warm up

  invisible(bloom_join(dat$x, dat$y, by = "id"))
  invisible(inner_join(dat$x, dat$y, by = "id"))

  result <- bench::mark(
    bloom = bloom_join(dat$x, dat$y, by = "id"),
    dplyr = inner_join(dat$x, dat$y, by = "id"),
    iterations = reps,
    check = FALSE,
    memory = TRUE
  )

  meta <- attr(bloom_join(dat$x, dat$y, by = "id"), "bloom_metadata")

  data.frame(
    n_x = format(n_x, big.mark = ",", scientific = FALSE),
    n_y = format(n_y, big.mark = ",", scientific = FALSE),
    overlap = paste0(overlap * 100, "%"),
    speed_ratio = round(as.numeric(result$median[2]) / as.numeric(result$median[1]), 2),
    mem_ratio = round(as.numeric(result$mem_alloc[2]) / as.numeric(result$mem_alloc[1]), 2),
    reduction = paste0(round(meta$reduction_ratio * 100, 0), "%")
  )
}

## ----correctness--------------------------------------------------------------
dat <- generate_data(5000, 500, 0.1)
bloom_result <- bloom_join(dat$x, dat$y, by = "id") |> arrange(id)
dplyr_result <- inner_join(dat$x, dat$y, by = "id") |> arrange(id)

# Strip bloom metadata for comparison
bloom_cmp <- as.data.frame(bloom_result)
attr(bloom_cmp, "bloom_metadata") <- NULL

all.equal(bloom_cmp, as.data.frame(dplyr_result), check.attributes = FALSE)

## ----benchmarks, message = FALSE, warning = FALSE-----------------------------
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

results <- do.call(rbind, lapply(scenarios, function(s) {
  run_bench(s[1], s[2], s[3], reps = 3)
}))

kable(results,
      col.names = c("n_x", "n_y", "overlap", "speed", "memory", "reduction"),
      align = "rrrrrr")

## ----tuning, message = FALSE--------------------------------------------------
dat <- generate_data(50000, 5000, 0.05)

fpr_results <- do.call(rbind, lapply(c(0.001, 0.01, 0.1), function(fpr) {
  result <- bench::mark(
    bloom_join(dat$x, dat$y, by = "id", fpr = fpr),
    iterations = 3,
    check = FALSE
  )
  data.frame(
    fpr = fpr,
    median_ms = round(as.numeric(result$median) * 1000, 1),
    mem_mb = round(as.numeric(result$mem_alloc) / 1024^2, 1)
  )
}))

kable(fpr_results, col.names = c("FPR", "Time (ms)", "Memory (MB)"))

