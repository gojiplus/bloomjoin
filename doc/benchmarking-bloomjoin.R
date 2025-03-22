## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  fig.width = 8,
  fig.height = 5,
  dpi = 96
)

## ----include=FALSE------------------------------------------------------------
library(tibble)
library(tidyverse)   # includes dplyr, tidyr, purrr, etc.
library(kableExtra)
library(bloomjoin)
library(reshape2)    # For dcast function
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

## ----include = TRUE-----------------------------------------------------------
# Function to generate test data with controllable overlap
generate_test_data <- function(n_x, n_y, overlap_pct = 0.5, key_cols = 1, value_cols = 3) {
  n_overlap <- round(min(n_x, n_y) * overlap_pct)
  
  if (key_cols == 1) {
    keys_x <- sample(1:(n_x * 10), n_x)
    keys_y <- c(sample(keys_x, n_overlap), sample(setdiff(1:(n_y * 10), keys_x), n_y - n_overlap))
  } else {
    keys_x <- list()
    keys_y <- list()
    for (i in 1:key_cols) {
      keys_x[[i]] <- sample(1:(n_x * 10), n_x)
      keys_y[[i]] <- c(
        sample(keys_x[[i]], n_overlap),
        sample(setdiff(1:(n_y * 10), keys_x[[i]]), n_y - n_overlap)
      )
      keys_y[[i]] <- sample(keys_y[[i]], n_y)
    }
    keys_x <- as.data.frame(keys_x)
    keys_y <- as.data.frame(keys_y)
    names(keys_x) <- names(keys_y) <- paste0("key", 1:key_cols)
  }
  
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
  
  for (i in 1:value_cols) {
    df_x[[paste0("x_val", i)]] <- rnorm(n_x)
    df_y[[paste0("y_val", i)]] <- rnorm(n_y)
  }
  
  return(list(x = df_x, y = df_y))
}


## ----include = TRUE-----------------------------------------------------------
# Function to run a single benchmark comparison
# Function to run a single benchmark comparison
run_benchmark <- function(n_x, n_y, overlap_pct = 0.5, key_cols = 1, value_cols = 3, 
                          join_type = "inner", times = 10, verbose = TRUE) {
  data <- generate_test_data(n_x, n_y, overlap_pct, key_cols, value_cols)
  df_x <- data$x
  df_y <- data$y
  
  by_cols <- if (key_cols == 1) "key" else paste0("key", 1:key_cols)
  bloom_verbose <- FALSE
  
  # Verify that both join methods produce equivalent results
  # Suppress warnings during verification
  bloom_result <- suppressWarnings(bloom_join(df_x, df_y, by = by_cols, type = join_type, verbose = FALSE))
  dplyr_result <- perform_standard_join(df_x, df_y, by = by_cols, type = join_type)
  
  equivalent <- nrow(bloom_result) == nrow(dplyr_result) && 
                all(sort(names(bloom_result)) == sort(names(dplyr_result)))
  
  if (!equivalent) {
    warning("Bloom join and dplyr join produced different results!")
  }
  
  # Suppress warnings during benchmarking
  suppressWarnings(
    bm <- microbenchmark(
      "bloom_join" = bloom_join(df_x, df_y, by = by_cols, type = join_type, verbose = bloom_verbose),
      "dplyr_join" = perform_standard_join(df_x, df_y, by = by_cols, type = join_type),
      times = times
    )
  )
  
  if (requireNamespace("data.table", quietly = TRUE)) {
    dt_x <- as.data.table(df_x)
    dt_y <- as.data.table(df_y)
    
    if (key_cols == 1) {
      setkeyv(dt_x, "key")
      setkeyv(dt_y, "key")
    } else {
      setkeyv(dt_x, paste0("key", 1:key_cols))
      setkeyv(dt_y, paste0("key", 1:key_cols))
    }
    
    suppressWarnings(
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
    )
    
    bm_combined <- rbind(bm, bm_dt)
    return(list(benchmark = bm_combined, equivalent = equivalent))
  }
  
  return(list(benchmark = bm, equivalent = equivalent))
}

# Function to run a series of benchmarks with varying parameters
run_benchmark_series <- function() {
  results <- data.frame()
  
  data_sizes <- list(
    c(1000000, 100000),
    c(10000000, 100000),
    c(1000000, 1000000)
  )
  
  overlaps <- c(0.1, 0.5, 0.9)
  join_types <- c("inner", "left")
  key_cols_list <- c(1, 2)
  
  for (size in data_sizes) {
    n_x <- size[1]
    n_y <- size[2]
    
    for (overlap in overlaps) {
      for (key_cols in key_cols_list) {
        for (join_type in join_types) {
          if (n_x * n_y > 1e9 && overlap < 0.5) {
            next
          }
          
          # Suppress warnings and capture output
          output <- capture.output({
            bm_result <- run_benchmark(n_x, n_y, overlap, key_cols, 3, join_type, times = 5)
          })
          
          bm <- bm_result$benchmark
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
          
          cat(sprintf("Completed: x=%d, y=%d, overlap=%.1f, keys=%d, type=%s\n", 
                      n_x, n_y, overlap, key_cols, join_type))
        }
      }
    }
  }
  
  return(results)
}

## ----include = TRUE-----------------------------------------------------------
# Function to run a series of benchmarks with varying parameters
run_benchmark_series <- function() {
  results <- data.frame()
  
  data_sizes <- list(
    c(1000000, 100000),
    c(10000000, 100000),
    c(1000000, 1000000)
  )
  
  overlaps <- c(0.1, 0.5, 0.9)
  join_types <- c("inner", "left")
  key_cols_list <- c(1, 2)
  
  for (size in data_sizes) {
    n_x <- size[1]
    n_y <- size[2]
    
    for (overlap in overlaps) {
      for (key_cols in key_cols_list) {
        for (join_type in join_types) {
          if (n_x * n_y > 1e9 && overlap < 0.5) {
            next
          }
          
          bm_result <- run_benchmark(n_x, n_y, overlap, key_cols, 3, join_type, times = 5)
          bm <- bm_result$benchmark
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
          
          cat(sprintf("Completed: x=%d, y=%d, overlap=%.1f, keys=%d, type=%s\n", 
                      n_x, n_y, overlap, key_cols, join_type))
        }
      }
    }
  }
  
  return(results)
}

## ----include = TRUE-----------------------------------------------------------
# Run the benchmark series
benchmark_results <- run_benchmark_series()

## -----------------------------------------------------------------------------
plot_results <- function(results) {
  # Filter only bloom_join and dplyr_join for comparison
  results_filtered <- results %>%
    filter(method %in% c("bloom_join", "dplyr_join"))
  
  # Calculate speedup using reshape2::dcast
  speedup_data <- reshape2::dcast(results_filtered, 
                                  n_x + n_y + overlap + key_cols + join_type ~ method, 
                                  value.var = "median_time_ms")
  
  speedup_data <- speedup_data %>%
    mutate(speedup = dplyr_join / bloom_join,
           size_ratio = n_x / n_y)
  
  # Overall speedup plot
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
         subtitle = "Point size represents speedup factor (dplyr time / bloom_join time)",
         x = "Number of rows in x", y = "Number of rows in y", color = "Performance") +
    theme_minimal() +
    scale_x_log10() + scale_y_log10()
  
  # Performance by overlap plot
  p2 <- ggplot(speedup_data, aes(x = as.factor(overlap), y = speedup, fill = as.factor(key_cols))) +
    geom_boxplot() +
    geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
    facet_wrap(~ join_type, labeller = labeller(
      join_type = c("inner" = "Inner Join", "left" = "Left Join")
    )) +
    labs(title = "Impact of Overlap Percentage on Bloom Join Performance",
         subtitle = "Values above 1 indicate bloom_join is faster than dplyr",
         x = "Overlap Percentage", y = "Speedup Factor (dplyr time / bloom_join time)",
         fill = "Number of Key Columns") +
    theme_minimal()
  
  # Absolute performance comparison plot
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
  
  list(speedup_overview = p1, overlap_impact = p2, absolute_performance = p3)
}

# Generate and display the plots
plots <- plot_results(benchmark_results)
plots$speedup_overview

## -----------------------------------------------------------------------------
plots$overlap_impact

## -----------------------------------------------------------------------------
plots$absolute_performance

## ----include = TRUE-----------------------------------------------------------
# Summarize when bloom_join is faster
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
      avg_overlap = mean(overlap),
      .groups = "drop"
    )
)

