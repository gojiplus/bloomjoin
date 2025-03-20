## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  fig.width = 8,
  fig.height = 5,
  dpi = 96
)

## ----include=FALSE------------------------------------------------------------
library(bloomjoin)
library(dplyr)
library(ggplot2)
library(microbenchmark)
library(data.table)

## ----include = TRUE-----------------------------------------------------------
# Helper function for standard joins that matches the bloom_join interface
perform_standard_join <- function(x, y, by = NULL, type = "inner") {
  if (type == "inner") {
    return(inner_join(x, y, by = by))
  } else if (type == "left") {
    return(left_join(x, y, by = by))
  } else if (type == "right") {
    return(right_join(x, y, by = by))
  } else if (type == "full") {
    return(full_join(x, y, by = by))
  } else if (type == "semi") {
    return(semi_join(x, y, by = by))
  } else if (type == "anti") {
    return(anti_join(x, y, by = by))
  } else {
    stop("Unsupported join type")
  }
}

# Function to generate test data with controllable overlap
generate_test_data <- function(n_x, n_y, overlap_pct = 0.5, key_cols = 1, value_cols = 3) {
  # Calculate the number of overlapping keys
  n_overlap <- round(min(n_x, n_y) * overlap_pct)
  
  # Generate keys for both data frames
  if (key_cols == 1) {
    # Single key column
    keys_x <- sample(1:(n_x * 10), n_x)
    keys_y <- c(sample(keys_x, n_overlap), sample(setdiff(1:(n_y * 10), keys_x), n_y - n_overlap))
  } else {
    # Multiple key columns
    keys_x <- list()
    keys_y <- list()
    for (i in 1:key_cols) {
      keys_x[[i]] <- sample(1:(n_x * 10), n_x)
      
      # Ensure proper overlap
      keys_y[[i]] <- c(
        sample(keys_x[[i]], n_overlap),
        sample(setdiff(1:(n_y * 10), keys_x[[i]]), n_y - n_overlap)
      )
      
      # Randomize the order for y
      keys_y[[i]] <- sample(keys_y[[i]], n_y)
    }
    
    # Convert to data frames
    keys_x <- as.data.frame(keys_x)
    keys_y <- as.data.frame(keys_y)
    names(keys_x) <- names(keys_y) <- paste0("key", 1:key_cols)
  }
  
  # Generate value columns
  df_x <- if (key_cols == 1) {
    data.frame(key = keys_x)
  } else {
    keys_x
  }
  
  df_y <- if (key_cols == 1) {
    data.frame(key = keys_y)
  } else {
    keys_y
  }
  
  # Add value columns
  for (i in 1:value_cols) {
    df_x[[paste0("x_val", i)]] <- rnorm(n_x)
    df_y[[paste0("y_val", i)]] <- rnorm(n_y)
  }
  
  return(list(x = df_x, y = df_y))
}

# Function to run a single benchmark comparison
run_benchmark <- function(n_x, n_y, overlap_pct = 0.5, key_cols = 1, value_cols = 3, 
                          join_type = "inner", times = 10, verbose = TRUE) {
  # Generate test data
  data <- generate_test_data(n_x, n_y, overlap_pct, key_cols, value_cols)
  df_x <- data$x
  df_y <- data$y
  
  # Define join columns
  by_cols <- if (key_cols == 1) "key" else paste0("key", 1:key_cols)
  
  # Turn off bloom_join's verbose output for the benchmark
  bloom_verbose <- FALSE
  
  # Run the benchmark
  if (verbose) {
    cat(sprintf("Benchmarking with:\n  - x rows: %d\n  - y rows: %d\n  - overlap: %.1f%%\n  - key columns: %d\n  - join type: %s\n",
                n_x, n_y, overlap_pct * 100, key_cols, join_type))
  }
  
  # First, verify that both joins produce the same result
  bloom_result <- bloom_join(df_x, df_y, by = by_cols, type = join_type, verbose = FALSE)
  dplyr_result <- perform_standard_join(df_x, df_y, by = by_cols, type = join_type)
  
  # Check if the results are equivalent (row count and column names should match)
  equivalent <- nrow(bloom_result) == nrow(dplyr_result) && 
                all(sort(names(bloom_result)) == sort(names(dplyr_result)))
  
  if (!equivalent) {
    warning("Bloom join and dplyr join produced different results!")
  }
  
  # Run the benchmark
  bm <- microbenchmark(
    "bloom_join" = bloom_join(df_x, df_y, by = by_cols, type = join_type, verbose = bloom_verbose),
    "dplyr_join" = perform_standard_join(df_x, df_y, by = by_cols, type = join_type),
    times = times
  )
  
  # If data.table is available, add it to the comparison
  if (requireNamespace("data.table", quietly = TRUE)) {
    dt_x <- as.data.table(df_x)
    dt_y <- as.data.table(df_y)
    
    # Set keys for data.table
    if (key_cols == 1) {
      setkeyv(dt_x, "key")
      setkeyv(dt_y, "key")
    } else {
      setkeyv(dt_x, paste0("key", 1:key_cols))
      setkeyv(dt_y, paste0("key", 1:key_cols))
    }
    
    # Add data.table to benchmark
    bm_dt <- microbenchmark(
      "data.table_join" = if (join_type == "inner") {
        dt_x[dt_y, nomatch = 0]
      } else if (join_type == "left") {
        dt_x[dt_y]
      } else if (join_type == "right") {
        dt_y[dt_x]
      } else {
        stop("Unsupported join type for data.table in this benchmark")
      },
      times = times
    )
    
    # Combine results
    bm_combined <- rbind(bm, bm_dt)
    return(list(benchmark = bm_combined, equivalent = equivalent))
  }
  
  return(list(benchmark = bm, equivalent = equivalent))
}

# Function to run a series of benchmarks with varying parameters
run_benchmark_series <- function() {
  # Create data frames to store results
  results <- data.frame()
  
  # Test different data sizes
  data_sizes <- list(
    c(10000, 1000),   # x larger than y
    c(1000, 10000),   # y larger than x
    c(100000, 1000),  # x much larger than y
    c(100000, 10000), # x larger than y, both large
    c(10000, 10000)   # equal size
  )
  
  # Test different overlaps
  overlaps <- c(0.1, 0.5, 0.9)
  
  # Test different join types
  join_types <- c("inner", "left")
  
  # Test different numbers of key columns
  key_cols_list <- c(1, 3)
  
  # Run benchmarks
  for (size in data_sizes) {
    n_x <- size[1]
    n_y <- size[2]
    
    for (overlap in overlaps) {
      for (key_cols in key_cols_list) {
        for (join_type in join_types) {
          # Skip combinations that would be too slow
          if (n_x * n_y > 1e9 && overlap < 0.5) {
            next
          }
          
          # Run the benchmark
          bm_result <- run_benchmark(n_x, n_y, overlap, key_cols, 3, join_type, times = 5)
          bm <- bm_result$benchmark
          
          # Extract and store results
          bm_summary <- summary(bm)
          
          for (i in 1:nrow(bm_summary)) {
            results <- rbind(results, data.frame(
              n_x = n_x,
              n_y = n_y,
              overlap = overlap,
              key_cols = key_cols,
              join_type = join_type,
              method = bm_summary$expr[i],
              median_time_ms = bm_summary$median[i] / 1e6,
              mean_time_ms = bm_summary$mean[i] / 1e6,
              min_time_ms = bm_summary$min[i] / 1e6,
              max_time_ms = bm_summary$max[i] / 1e6,
              n_eval = bm_summary$neval[i],
              equivalent = bm_result$equivalent
            ))
          }
          
          # Print progress
          cat(sprintf("Completed: x=%d, y=%d, overlap=%.1f, keys=%d, type=%s\n", 
                      n_x, n_y, overlap, key_cols, join_type))
        }
      }
    }
  }
  
  return(results)
}

# Run the benchmark series
benchmark_results <- run_benchmark_series()

# Save results
write.csv(benchmark_results, "bloom_join_benchmark_results.csv", row.names = FALSE)

# Create visualization
plot_results <- function(results) {
  # Filter only bloom_join and dplyr_join for comparison
  results_filtered <- results[results$method %in% c("bloom_join", "dplyr_join"), ]
  
  # Calculate speedup
  speedup_data <- reshape2::dcast(results_filtered, 
                                 n_x + n_y + overlap + key_cols + join_type ~ method, 
                                 value.var = "median_time_ms")
  
  speedup_data$speedup <- speedup_data$dplyr_join / speedup_data$bloom_join
  speedup_data$size_ratio <- speedup_data$n_x / speedup_data$n_y
  speedup_data$total_rows <- speedup_data$n_x + speedup_data$n_y
  
  # Plot overall speedup
  p1 <- ggplot(speedup_data, aes(x = n_x, y = n_y, size = speedup, color = speedup > 1)) +
    geom_point(alpha = 0.7) +
    scale_color_manual(values = c("red", "blue"), 
                       labels = c("Slower than dplyr", "Faster than dplyr")) +
    scale_size_continuous(name = "Speedup factor") +
    facet_grid(join_type ~ key_cols, labeller = labeller(
      join_type = c("inner" = "Inner Join", "left" = "Left Join"),
      key_cols = c("1" = "1 Key Column", "3" = "3 Key Columns")
    )) +
    labs(title = "Bloom Join vs dplyr Join Performance",
         subtitle = "Size of point represents speedup factor (bloom_join time / dplyr join time)",
         x = "Number of rows in x", y = "Number of rows in y", color = "Performance") +
    theme_minimal() +
    scale_x_log10() + scale_y_log10()
  
  # Plot performance by overlap percentage
  p2 <- ggplot(speedup_data, aes(x = as.factor(overlap), y = speedup, fill = as.factor(key_cols))) +
    geom_boxplot() +
    geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
    facet_wrap(~ join_type, labeller = labeller(
      join_type = c("inner" = "Inner Join", "left" = "Left Join")
    )) +
    labs(title = "Impact of Overlap Percentage on Bloom Join Performance",
         subtitle = "Values above 1 indicate bloom_join is faster than dplyr",
         x = "Overlap Percentage", y = "Speedup Factor (dplyr time / bloom time)",
         fill = "Number of Key Columns") +
    theme_minimal()
  
  # Plot absolute performance comparison
  p3 <- ggplot(results_filtered, aes(x = paste(n_x, "x", n_y), y = median_time_ms, fill = method)) +
    geom_bar(stat = "identity", position = "dodge") +
    facet_grid(join_type ~ key_cols, scales = "free_y", labeller = labeller(
      join_type = c("inner" = "Inner Join", "left" = "Left Join"),
      key_cols = c("1" = "1 Key Column", "3" = "3 Key Columns")
    )) +
    labs(title = "Absolute Performance Comparison",
         x = "Data Size (x rows x y rows)", y = "Median Time (ms)",
         fill = "Method") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  # Return list of plots
  return(list(speedup_overview = p1, overlap_impact = p2, absolute_performance = p3))
}

# Generate plots
plots <- plot_results(benchmark_results)

# Save plots
ggsave("bloom_join_speedup_overview.png", plots$speedup_overview, width = 10, height = 8)
ggsave("bloom_join_overlap_impact.png", plots$overlap_impact, width = 10, height = 6)
ggsave("bloom_join_absolute_performance.png", plots$absolute_performance, width = 12, height = 8)

# Print a summary of when bloom_join is faster
speedup_summary <- benchmark_results %>%
  filter(method %in% c("bloom_join", "dplyr_join")) %>%
  select(n_x, n_y, overlap, key_cols, join_type, method, median_time_ms) %>%
  reshape2::dcast(n_x + n_y + overlap + key_cols + join_type ~ method, value.var = "median_time_ms") %>%
  mutate(speedup = dplyr_join / bloom_join,
         is_faster = speedup > 1,
         size_ratio = n_x / n_y)

cat("\nBloom join was faster in", sum(speedup_summary$is_faster), 
    "out of", nrow(speedup_summary), "test cases\n")

cat("\nAverage speedup when bloom_join is faster:", 
    mean(speedup_summary$speedup[speedup_summary$is_faster]), "\n")

cat("\nConditions where bloom_join tends to be faster:\n")
print(
  speedup_summary %>%
    filter(is_faster) %>%
    group_by(join_type, key_cols) %>%
    summarize(
      avg_speedup = mean(speedup),
      count = n(),
      avg_x_rows = mean(n_x),
      avg_y_rows = mean(n_y),
      avg_size_ratio = mean(size_ratio),
      avg_overlap = mean(overlap)
    )
)

# Advanced analysis: fit a model to predict when bloom_join is faster
model <- glm(is_faster ~ log(n_x) + log(n_y) + overlap + key_cols + join_type,
             data = speedup_summary, family = binomial())

cat("\nFactors predicting when bloom_join is faster:\n")
print(summary(model)$coefficients)

