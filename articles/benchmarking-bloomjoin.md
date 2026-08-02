# Performance Analysis of Bloom Filter Joins

## Overview

This vignette compares `bloomjoin` against standard `dplyr` joins on two
dimensions: **speed** and **memory**. Bloom joins excel when tables are
asymmetric (large left, small right) with low overlap.

``` r

library(bloomjoin)
library(dplyr)
library(bench)
library(knitr)
```

## Correctness

First, verify that [`bloom_join()`](../reference/bloom_join.md) produces
identical results to `dplyr`:

``` r

dat <- generate_data(5000, 500, 0.1)
bloom_result <- bloom_join(dat$x, dat$y, by = "id") |> arrange(id)
dplyr_result <- inner_join(dat$x, dat$y, by = "id") |> arrange(id)

# Strip bloom metadata for comparison
bloom_cmp <- as.data.frame(bloom_result)
attr(bloom_cmp, "bloom_metadata") <- NULL

all.equal(bloom_cmp, as.data.frame(dplyr_result), check.attributes = FALSE)
#> [1] TRUE
```

## Speed and Memory Benchmarks

We test across scenarios varying table asymmetry and overlap.
`speed_ratio` and `mem_ratio` \> 1 means bloom wins.

``` r

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
```

|       n_x |     n_y | overlap | speed | memory | reduction |
|----------:|--------:|--------:|------:|-------:|----------:|
| 1,000,000 |  10,000 |      1% |  3.03 |   2.23 |       99% |
| 1,000,000 |  10,000 |      5% |  1.50 |   2.08 |       95% |
|   500,000 |   5,000 |      2% |  1.41 |   2.03 |       98% |
|   500,000 |   5,000 |     10% |  1.25 |   1.67 |       90% |
|   200,000 |  20,000 |      5% |  0.95 |   1.19 |       95% |
|   200,000 |  20,000 |     25% |  0.79 |   0.95 |       75% |
|   100,000 | 100,000 |     10% |  0.33 |   0.40 |       89% |
|   100,000 | 100,000 |     50% |  0.41 |   0.43 |       50% |

## Interpretation

- **speed, memory \> 1**: bloom wins (dplyr_time / bloom_time)
- **reduction**: % of rows filtered out before join — explains memory
  savings

High reduction means fewer rows held in memory during the join. When
reduction is low, Bloom filter overhead dominates and dplyr wins.

## When to Use Bloom Joins

| Condition                | Bloom benefit    |
|--------------------------|------------------|
| Large x, small y (10:1+) | Strong           |
| Low overlap (\<25%)      | Strong           |
| Equal-sized tables       | None (use dplyr) |
| High overlap (\>50%)     | None (use dplyr) |

## Tuning

The `fpr` parameter controls the false positive rate. Lower values
reduce false positives but increase filter size:

``` r

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
```

|   FPR | Time (ms) | Memory (MB) |
|------:|----------:|------------:|
| 0.001 |       4.8 |         3.3 |
| 0.010 |       4.8 |         3.2 |
| 0.100 |       5.0 |         3.4 |

Default `fpr = 0.01` balances speed and memory well.
