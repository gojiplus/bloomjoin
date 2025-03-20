# BloomJoin

An R package implementing Bloom filter-based joins for improved performance with large datasets.

## Overview

BloomJoin provides an alternative join implementation for R that uses a hash-based approach inspired by Bloom filters to optimize the performance of joins between data frames. Traditional joins in R can be inefficient when dealing with large datasets, especially when one table is significantly larger than the other and the join key selectivity is low.

## Installation

```r
# Install from GitHub
devtools::install_github("yourusername/bloomjoin")
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

We conducted extensive benchmarks comparing BloomJoin to standard dplyr joins and data.table joins across various scenarios. Here's an honest assessment of when BloomJoin provides benefits:

### When BloomJoin Performs Better

BloomJoin was faster in specific scenarios:

| Join Type | Key Columns | Avg Speedup | Count | Avg X Rows | Avg Y Rows | Avg Size Ratio | Avg Overlap |
|-----------|------------|-------------|-------|------------|------------|----------------|-------------|
| left      | 1          | 1.41        | 5     | 100,000    | 4,600      | 64             | 0.42        |
| left      | 3          | 1.07        | 1     | 100,000    | 10,000     | 10             | 0.5         |

The data suggests that BloomJoin offers the most significant advantages when:

1. The primary table (x) is much larger than the lookup table (y) (high size ratio)
2. You're performing left joins rather than inner joins
3. Using a single key column rather than composite keys
4. The overlap between the tables is moderate (around 40-50%)

### Statistical Analysis

Our predictive model identified factors that influence BloomJoin performance:

```
                   Estimate   Std. Error       z value   Pr(>|z|)
(Intercept)   -1.273048e+02 2.519650e+04 -5.052479e-03 0.99596872
log(n_x)       9.526247e+00 2.112624e+03  4.509201e-03 0.99640219
log(n_y)       2.117300e-16 7.006168e-01  3.022051e-16 1.00000000
overlap       -2.513696e+00 2.806205e+00 -8.957635e-01 0.37037907
key_cols      -1.826039e+00 9.317801e-01 -1.959732e+00 0.05002714
join_typeleft  2.253877e+01 6.578793e+03  3.425973e-03 0.99726647
```

While most coefficients didn't reach statistical significance (likely due to limited sample size), the key insights are:

- Negative effect of multiple key columns (p = 0.05) suggests BloomJoin works better with single-column keys
- Negative coefficient for overlap suggests lower overlap increases BloomJoin's advantage (though not statistically significant)
- Positive coefficient for left joins suggests better performance for left joins than inner joins

## Recommendations for Use

Based on our benchmarks, we recommend using BloomJoin when:

1. You have a large primary table joining against a much smaller lookup table
2. You're performing a left join where many keys from the primary table may not exist in the lookup table
3. You're joining on a single column rather than multiple columns
4. The join operation is a performance bottleneck in your workflow

In other scenarios, standard dplyr joins or data.table may provide comparable or better performance.

## Limitations

1. BloomJoin is optimized for certain join scenarios and may not outperform standard joins in all cases
2. The implementation doesn't use actual Bloom filters but a hash-based approach for simplicity
3. Currently most efficient for inner and left joins; less optimized for other join types
4. Performance benefits decrease with composite keys (multiple join columns)

## Future Work

1. Implement true Bloom filters for potentially better memory efficiency
2. Optimize for composite keys and other join types
3. Parallel processing for hash creation and filtering
4. Automatic parameter tuning based on input data characteristics

## License

MIT

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.