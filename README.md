# BloomJoin: Bloom Filter Based Joins

An R package implementing Bloom filter-based joins for improved performance with large datasets.

## Overview

BloomJoin provides an alternative join implementation for R that uses an actual Bloom filter, implemented in C++ via Rcpp, to optimize the performance of joins between data frames. Traditional joins in R can be inefficient when dealing with large datasets, especially when one table is significantly larger than the other and the join key selectivity is low.

## Installation

```r
# Install from GitHub
devtools::install_github("gojiplus/bloomjoin")
```

## Documentation

The full pkgdown site, including function reference, articles, and release notes, is published automatically to GitHub Pages whenever changes are pushed to the main branch. You can browse it at <https://soodoku.github.io/bloomjoin/>. To rebuild the site locally run:

```r
pkgdown::build_site()
```

## Usage

```r
library(bloomjoin)

# Basic usage
result <- bloom_join(df1, df2, by = "id", type = "inner")

# With multiple join columns
result <- bloom_join(df1, df2, by = c("id", "date"), type = "left")

# With performance tuning parameters
result <- bloom_join(df1, df2,
                    by = "id",
                    type = "inner",
                    engine = "bloom",
                    prefilter_side = "auto",
                    fpr = 0.001,
                    n_hint = list(y = 50000),
                    verbose = TRUE)
```

### Proof of Correctness

```r
library(dplyr)
library(tibble)

set.seed(123)
left <- tibble(id = sample(1:5000, 4000), value_left = rnorm(4000))
right <- tibble(id = sample(1:5000, 1500), value_right = rnorm(1500))

bloom <- bloom_join(left, right, by = "id") %>% arrange(id)
reference <- inner_join(left, right, by = "id") %>% arrange(id)

stopifnot(identical(bloom, reference))
```

### Benchmarking Time and Memory

```r
library(bench)

bench::mark(
  bloom_join = bloom_join(left, right, by = "id"),
  dplyr_join = inner_join(left, right, by = "id"),
  iterations = 5,
  check = FALSE
)
```

For a ready-to-run demonstration that generates data, verifies correctness, and
prints benchmark summaries, execute:

```sh
Rscript inst/scripts/usage-and-benchmark.R
```

## How It Works

BloomJoin uses a Bloom filter pre-processing step to optimize joins:

1. Sample the join keys to estimate distinct counts and decide which table to pre-filter.
2. Build a Bloom filter (using a cache-friendly bitset stored in C++) from the chosen build-side join keys.
3. Probe the filter with the opposing table to remove rows that cannot match before delegating to the appropriate `dplyr` verb.

Because Bloom filters may produce false positives but never false negatives, this pre-filtering step safely reduces the number of rows that participate in the expensive join while preserving all possible matches. The underlying Bloom filter implementation is provided by a compiled Rcpp module for performance.

## Performance Benchmarks

See [here](https://htmlpreview.github.io/?https://github.com/gojiplus/bloomjoin/blob/main/doc/benchmarking-bloomjoin.html)

## Future Work

1. Explore alternative probabilistic data structures (e.g., binary-fuse filters) for further memory savings
2. Parallel processing for Bloom filter construction and probing
3. Streaming/Chunked probing for very large inputs

## License

MIT

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.
