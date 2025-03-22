# BloomJoin: Bloom Filter Based Joins

An R package implementing Bloom filter-based joins for improved performance with large datasets.

## Overview

BloomJoin provides an alternative join implementation for R that uses a hash-based approach inspired by Bloom filters to optimize the performance of joins between data frames. Traditional joins in R can be inefficient when dealing with large datasets, especially when one table is significantly larger than the other and the join key selectivity is low.

## Installation

```r
# Install from GitHub
devtools::install_github("gojiplus/bloomjoin")
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
                    bloom_size = 1000000, 
                    false_positive_rate = 0.001,
                    verbose = TRUE)
```

## How It Works

BloomJoin uses a hash-based approach to optimize joins:

1. Create a hash set of all keys from the lookup table (y)
2. Filter the primary table (x) to only include rows with keys that exist in the hash set
3. Perform a standard join on the filtered dataset

This pre-filtering step can significantly reduce the size of the join operation when many keys in the primary table don't exist in the lookup table.

## Performance Benchmarks

See [here](https://htmlpreview.github.io/?https://github.com/gojiplus/bloomjoin/blob/main/doc/benchmarking-bloomjoin.html)

## Future Work

1. Implement true Bloom filters for potentially better memory efficiency
2. Optimize for composite keys and other join types
3. Parallel processing for hash creation and filtering
4. Automatic parameter tuning based on input data characteristics

## License

MIT

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.
