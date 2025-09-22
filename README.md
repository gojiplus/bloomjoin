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
