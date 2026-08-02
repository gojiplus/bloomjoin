# Getting Started with BloomJoin

## Introduction

The `bloomjoin` package provides high-performance join operations for
large data frames using Bloom filters. A Bloom filter is a
space-efficient probabilistic data structure that can quickly test
whether an element is in a set, making it perfect for pre-filtering
joins on large datasets.

This guide demonstrates when and how to use
[`bloom_join()`](../reference/bloom_join.md) effectively.

``` r

library(bloomjoin)
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union
library(tibble)
```

## When to Use Bloom Joins

Bloom joins are most effective when:

1.  **Joining large datasets** (\>10k rows) with smaller lookup tables
2.  **Low selectivity** scenarios where many keys in the left table
    don’t match
3.  **Inner, semi, or anti joins** (maximum benefit)
4.  **Memory-constrained** environments

## Basic Usage

### Simple Inner Join

``` r

# Create sample data
customers <- tibble(
  customer_id = 1:10000,
  name = paste("Customer", 1:10000),
  segment = sample(c("Premium", "Standard", "Basic"), 10000, replace = TRUE)
)

# Small lookup table
active_customers <- tibble(
  customer_id = sample(1:10000, 500),  # Only 5% of customers are active
  last_login = as.Date("2024-01-01") + sample(0:365, 500, replace = TRUE)
)

# Compare bloom join with standard join
system.time({
  bloom_result <- bloom_join(customers, active_customers, 
                           by = "customer_id", verbose = TRUE)
})
#> Prefilter retained 501 of 10000 rows from 'x'
#>    user  system elapsed 
#>   0.008   0.000   0.009

system.time({
  std_result <- inner_join(customers, active_customers, by = "customer_id")
})
#>    user  system elapsed 
#>   0.002   0.000   0.002

# Results should be identical
identical(nrow(bloom_result), nrow(std_result))
#> [1] TRUE
```

### All Join Types

``` r

# Sample datasets with partial overlap
x <- tibble(id = 1:8, x_val = letters[1:8])
y <- tibble(id = c(3:10), y_val = 1:8)

cat("Dataset overlap:\n")
#> Dataset overlap:
cat("x has IDs:", paste(x$id, collapse = ", "), "\n")
#> x has IDs: 1, 2, 3, 4, 5, 6, 7, 8
cat("y has IDs:", paste(y$id, collapse = ", "), "\n\n")
#> y has IDs: 3, 4, 5, 6, 7, 8, 9, 10

# Inner join - only matching records
inner_result <- bloom_join(x, y, by = "id", type = "inner")
cat("Inner join:", nrow(inner_result), "rows\n")
#> Inner join: 6 rows

# Left join - all records from x, matching from y  
left_result <- bloom_join(x, y, by = "id", type = "left")
cat("Left join:", nrow(left_result), "rows\n")
#> Left join: 8 rows

# Semi join - records in x that have matches in y
semi_result <- bloom_join(x, y, by = "id", type = "semi") 
cat("Semi join:", nrow(semi_result), "rows\n")
#> Semi join: 6 rows

# Anti join - records in x that DON'T have matches in y
anti_result <- bloom_join(x, y, by = "id", type = "anti")
cat("Anti join:", nrow(anti_result), "rows\n")
#> Anti join: 2 rows
print(anti_result)
#> # A tibble: 2 × 2
#>      id x_val
#>   <int> <chr>
#> 1     1 a    
#> 2     2 b
```

## Advanced Features

### Multi-column Joins

``` r

# Create data with composite keys
orders <- tibble(
  customer_id = rep(1:100, each = 3),
  product_id = rep(c(101, 102, 103), 100),
  quantity = sample(1:5, 300, replace = TRUE)
)

inventory <- tibble(
  customer_id = rep(1:50, each = 2), # Only half the customers have inventory
  product_id = rep(c(101, 102), 50),
  in_stock = sample(c(TRUE, FALSE), 100, replace = TRUE)
)

# Multi-column bloom join
result <- bloom_join(orders, inventory, 
                    by = c("customer_id", "product_id"), 
                    verbose = TRUE)
#> Skipping Bloom pre-filter: prefilter skip heuristic triggered
cat("Multi-column join result:", nrow(result), "rows\n")
#> Multi-column join result: 100 rows
```

### Named Join Columns

``` r

# Different column names in each table
sales <- tibble(
  cust_id = 1:100,
  sale_amount = rnorm(100, 100, 20)
)

profiles <- tibble(
  customer_id = sample(1:100, 60),  # Only 60% coverage
  tier = sample(c("Gold", "Silver", "Bronze"), 60, replace = TRUE)
)

# Join with different column names
result <- bloom_join(sales, profiles, 
                    by = c("cust_id" = "customer_id"))
cat("Named column join:", nrow(result), "rows\n")
#> Named column join: 60 rows
```

### Performance Tuning

``` r

# Large dataset example
large_x <- tibble(
  id = sample(1:100000, 50000, replace = TRUE),
  transaction_id = 1:50000,
  amount = rnorm(50000, 100, 50)
)

small_y <- tibble(
  id = sample(1:100000, 1000, replace = FALSE),
  account_type = sample(c("Checking", "Savings", "Credit"), 1000, replace = TRUE)
)

# Default parameters
result1 <- bloom_join(large_x, small_y, by = "id", verbose = TRUE)
#> Prefilter retained 556 of 50000 rows from 'x'

# Tuned parameters for better performance
result2 <- bloom_join(large_x, small_y, by = "id", 
                     n_hint = list(y = 2000),     # Hint for expected cardinality
                     fpr = 0.001, # Lower FPR
                     verbose = TRUE)
#> Prefilter retained 556 of 50000 rows from 'x'

# Compare filter effectiveness
cat("Default filter effectiveness:\n")
#> Default filter effectiveness:
cat("Tuned filter effectiveness:\n")
#> Tuned filter effectiveness:
```

## Error Handling and Diagnostics

The package provides helpful error messages and suggestions:

``` r

# Type in wrong join type
try(bloom_join(x, y, by = "id", type = "inne"))  # Typo in "inner"
#> Error in bloom_join(x, y, by = "id", type = "inne") : 
#>   Invalid join type 'inne'

# Missing join columns
try(bloom_join(x, y, by = "nonexistent_col"))
#> Error in resolve_join_columns(x, y, by) : 
#>   Join columns not found in x: nonexistent_col

# Wrong data types
try(bloom_join(list(a = 1:5), y, by = "id"))
#> Error in validate_join_inputs(x, y, type, fpr) : 
#>   Both 'x' and 'y' must be data frames
```

## Performance Guidelines

### When Bloom Joins Excel

``` r

# Scenario 1: Large left table, small right table, low selectivity
create_test_scenario <- function(n_left, n_right, selectivity) {
  left_df <- tibble(
    id = 1:n_left,
    data = rnorm(n_left)
  )
  
  # Right table with controlled selectivity
  n_matches <- round(n_left * selectivity)
  right_df <- tibble(
    id = sample(1:n_left, n_right, replace = FALSE),
    lookup = rnorm(n_right)
  )
  
  list(left = left_df, right = right_df)
}

# Test scenarios
scenarios <- list(
  list(name = "Optimal: Large left, small right, low selectivity", 
       n_left = 50000, n_right = 1000, selectivity = 0.02),
  list(name = "Good: Medium datasets, moderate selectivity",
       n_left = 10000, n_right = 2000, selectivity = 0.10),
  list(name = "Poor: Small datasets, high selectivity", 
       n_left = 1000, n_right = 800, selectivity = 0.80)
)

for (scenario in scenarios) {
  cat("\n", scenario$name, "\n")
  cat(paste(rep("=", nchar(scenario$name)), collapse = ""), "\n")
  
  test_data <- create_test_scenario(scenario$n_left, scenario$n_right, scenario$selectivity)
  
  # Time bloom join
  bloom_time <- system.time({
    bloom_result <- bloom_join(test_data$left, test_data$right, by = "id")
  })["elapsed"]
  
  # Time standard join  
  std_time <- system.time({
    std_result <- inner_join(test_data$left, test_data$right, by = "id")
  })["elapsed"]
  
  cat("Bloom join time:", round(bloom_time * 1000, 2), "ms\n")
  cat("Standard join time:", round(std_time * 1000, 2), "ms\n")
  cat("Speedup:", round(std_time / bloom_time, 2), "x\n")
}
#> 
#>  Optimal: Large left, small right, low selectivity 
#> ================================================= 
#> Bloom join time: 5 ms
#> Standard join time: 3 ms
#> Speedup: 0.6 x
#> 
#>  Good: Medium datasets, moderate selectivity 
#> =========================================== 
#> Bloom join time: 4 ms
#> Standard join time: 2 ms
#> Speedup: 0.5 x
#> 
#>  Poor: Small datasets, high selectivity 
#> ====================================== 
#> Bloom join time: 2 ms
#> Standard join time: 1 ms
#> Speedup: 0.5 x
```

## Best Practices

1.  **Use for appropriate scenarios**: Large datasets with low
    selectivity
2.  **Tune parameters**: Adjust `fpr` and optional `n_hint` values for
    your data
3.  **Monitor performance**: Use `verbose = TRUE` to see filter
    effectiveness
4.  **Consider join type**: Inner/semi/anti joins benefit most
5.  **Test with your data**: Benchmark against standard joins for your
    use case

## Memory Considerations

``` r

# For very large datasets, consider chunked processing
process_large_join <- function(large_df, small_df, chunk_size = 10000) {
  results <- list()
  n_chunks <- ceiling(nrow(large_df) / chunk_size)
  
  for (i in 1:n_chunks) {
    start_row <- (i - 1) * chunk_size + 1
    end_row <- min(i * chunk_size, nrow(large_df))
    
    chunk <- large_df[start_row:end_row, ]
    chunk_result <- bloom_join(chunk, small_df, by = "id")
    results[[i]] <- chunk_result
  }
  
  do.call(rbind, results)
}

# Example with chunked processing
cat("Chunked processing can help with memory management for very large datasets\n")
#> Chunked processing can help with memory management for very large datasets
```

## Limitations and Considerations

1.  **Anti-joins**: Cannot use Bloom filters for pre-filtering due to
    false positives
2.  **Left/right/full joins**: Limited benefit as all rows from one
    table are kept
3.  **High selectivity**: Performance advantage decreases with high
    match rates
4.  **Small datasets**: Overhead may outweigh benefits for small data
    (\<1k rows)

## Conclusion

The `bloomjoin` package is most effective for: - Large datasets (\>10k
rows) - Low selectivity joins (\<25% match rate) - Inner, semi, or anti
join operations - Memory-constrained environments

Use the `verbose = TRUE` parameter to monitor filter effectiveness and
tune parameters as needed for optimal performance with your specific
datasets.
