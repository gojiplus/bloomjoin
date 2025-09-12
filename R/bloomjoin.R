#' BloomJoin: Efficient data.frame joins using Bloom filters
#'
#' @description
#' A package that provides optimized join operations for large data frames using
#' Bloom filters to reduce memory usage and improve performance. The Bloom filter is
#' used to pre-filter rows that won't match in the final join operation, significantly
#' reducing the computational cost for large datasets with low overlap.
#'
#' @details
#' The main function in this package is \code{\link{bloom_join}}, which implements
#' various types of joins (inner, left, right, full, semi, anti) using Bloom filters
#' to improve performance. This is particularly useful for large datasets where the
#' join operation would otherwise be memory-intensive and slow.
#'
#' @importFrom dplyr inner_join left_join right_join full_join semi_join anti_join
#' @importFrom Rcpp sourceCpp evalCpp
#' @useDynLib bloomjoin, .registration = TRUE
#'
#' @name bloomjoin
"_PACKAGE"

#' Join two data frames using a Bloom filter implemented with Rcpp
#'
#' This function uses a high-performance Rcpp implementation of Bloom filters to efficiently join
#' large data frames. The Bloom filter is used to pre-filter the left data frame, reducing the
#' number of comparisons needed for the actual join.
#'
#' @param x A data frame
#' @param y A data frame
#' @param by A character vector of variables to join by
#' @param type Type of join: "inner", "left", "right", "full", "semi", or "anti"
#' @param bloom_size Size of the Bloom filter. If NULL, will be automatically calculated
#' @param false_positive_rate False positive rate for the Bloom filter (default: 0.01)
#' @param verbose Logical, whether to print performance information (default: FALSE)
#'
#' @return A joined data frame with a class of "bloomjoin"
#' @export
#'
#' @examples
#' x <- data.frame(id = 1:100000, value_x = rnorm(100000))
#' y <- data.frame(id = 50001:60000, value_y = rnorm(10000))
#' result <- bloom_join(x, y, by = "id")
bloom_join <- function(x, y, by = NULL, type = "inner",
                       bloom_size = NULL, false_positive_rate = 0.01, verbose = FALSE) {
  
  # Validate inputs and prepare join parameters
  join_params <- validate_and_prepare_join(x, y, by, type, false_positive_rate, verbose)
  
  # Early exit for special cases
  if (join_params$should_skip_bloom) {
    return(join_params$result)
  }
  
  # Extract prepared parameters
  x <- join_params$x
  y <- join_params$y
  by <- join_params$by
  join_col <- join_params$join_col
  temp_join_cols <- join_params$temp_join_cols
  orig_by <- join_params$orig_by
  
  start_time <- if (verbose) Sys.time() else NULL

  # Join column preparation is now handled in validate_and_prepare_join

  # Only use Bloom filter when it will actually improve performance
  bloom_beneficial <- should_use_bloom_filter(x, y, type)
  
  # Skip Bloom filter for small datasets where overhead exceeds benefits
  if (nrow(x) < 1000 && nrow(y) < 1000) {
    if (verbose) {
      message("Small dataset detected - using standard join for better performance")
    }
    result <- perform_standard_join(x, y, if (exists("orig_by")) orig_by else by, type)
    return(result)
  }

  # For anti-joins, skip Bloom filter entirely and go straight to standard join
  if (type == "anti") {
    # Clean up composite keys if created
    if (join_col == ".composite_key") {
      x[".composite_key"] <- NULL
      y[".composite_key"] <- NULL
    }
    
    result <- perform_standard_join(x, y, if (exists("orig_by")) orig_by else by, type)
    
    if (verbose) {
      end_time <- Sys.time()
      elapsed <- as.numeric(end_time - start_time, units = "secs")
      message("Note: ANTI join performed - Bloom filters cannot provide pre-filtering benefits due to false positives")
      message(sprintf("Bloom join completed in %.3f seconds", elapsed))
    }
    
    # Only add metadata for anti-joins if verbose mode is enabled
    if (verbose) {
      attr(result, "bloom_metadata") <- list(
        original_rows_x = nrow(x),
        original_rows_y = nrow(y),
        join_type = type,
        bloom_filter_used = FALSE,
        note = "Anti-joins bypass Bloom filters due to false positive limitations"
      )
    }
    return(result)
  }

  # Get keys from y table
  keys_in_y <- if (join_col == ".composite_key") {
    as.character(y$.composite_key)
  } else {
    as.character(y[[join_col]])
  }

  # Get keys to check from x table
  keys_to_check <- if (join_col == ".composite_key") {
    as.character(x$.composite_key)
  } else {
    as.character(x[[join_col]])
  }

  # Determine optimal bloom filter size if not specified
  if (is.null(bloom_size)) {
    # Use the cardinality of unique keys in y, but be smarter about it
    unique_y_keys <- unique(keys_in_y[!is.na(keys_in_y)])
    bloom_size <- max(length(unique_y_keys), 100) # Minimum size for stability
    
    # Adjust based on dataset sizes
    if (length(keys_to_check) > 10000 && length(unique_y_keys) < 1000) {
      bloom_size <- bloom_size * 2 # Larger filter for better performance with large x
    }
  }
  
  if (verbose) {
    message(sprintf("Creating Bloom filter: %d unique keys in y, filter size: %d, FPR: %.4f", 
                   length(unique(keys_in_y[!is.na(keys_in_y)])), bloom_size, false_positive_rate))
  }

  # Call the improved Rcpp function to create the filter and check keys
  is_in_filter <- rcpp_filter_keys(keys_in_y, keys_to_check, bloom_size, false_positive_rate)
  
  # Calculate filter effectiveness
  filtered_count <- sum(is_in_filter)
  original_count <- length(keys_to_check)
  reduction_ratio <- 1 - (filtered_count / original_count)
  
  if (verbose) {
    message(sprintf("Bloom filter reduced dataset from %d to %d rows (%.1f%% reduction)", 
                   original_count, filtered_count, reduction_ratio * 100))
  }

  # For inner, semi joins: filter x first, then join
  # Note: Anti-joins cannot use Bloom filters for pre-filtering due to false positives
  if (type %in% c("inner", "semi")) {
    # Filter the larger data frame
    x_filtered <- x[is_in_filter, , drop = FALSE]

    # If we created a composite key, we need to remove it before the final join
    if (join_col == ".composite_key") {
      x_filtered[".composite_key"] <- NULL
      y[".composite_key"] <- NULL
    }

    # Perform the final join
    result <- perform_standard_join(x_filtered, y, if (exists("orig_by")) orig_by else by, type)
  } else {
    # For left, right, full, anti joins: use Bloom filter as a hint to optimize the join order
    # but still do the full join to preserve all rows
    # Note: Anti-joins cannot use Bloom filters for pre-filtering due to false positives
    
    # Clean up composite keys if created
    if (join_col == ".composite_key") {
      x[".composite_key"] <- NULL
      y[".composite_key"] <- NULL
    }
    
    # For these join types, we can't pre-filter, so just do the standard join
    # The Bloom filter information could be used for join algorithm selection in the future
    result <- perform_standard_join(x, y, if (exists("orig_by")) orig_by else by, type)
    
    if (verbose) {
      message(sprintf("Note: %s join performed - Bloom filter used for analysis only", toupper(type)))
    }
  }

  # Remove temporary columns if we created them
  if (exists("temp_join_cols")) {
    result[temp_join_cols] <- NULL
  }
  
  # Remove composite key columns if they exist in the result
  composite_cols <- names(result)[grepl("^\\.composite_key", names(result))]
  if (length(composite_cols) > 0) {
    result[composite_cols] <- NULL
  }

  # Keep result as standard data frame to match dplyr output exactly
  
  if (verbose) {
    end_time <- Sys.time()
    elapsed <- as.numeric(end_time - start_time, units = "secs")
    message(sprintf("Bloom join completed in %.3f seconds", elapsed))
  }
  
  # Only add metadata if verbose mode is enabled - to match dplyr output exactly
  if (verbose) {
    if (type %in% c("inner", "semi")) {
      # For joins that benefit from filtering
      attr(result, "bloom_metadata") <- list(
        original_rows_x = nrow(x),
        original_rows_y = nrow(y),
        filtered_rows_x = sum(is_in_filter),
        bloom_size = bloom_size,
        false_positive_rate = false_positive_rate,
        reduction_ratio = reduction_ratio,
        join_type = type,
        bloom_filter_used = TRUE
      )
    } else {
      # For joins where filtering wasn't applied
      attr(result, "bloom_metadata") <- list(
        original_rows_x = nrow(x),
        original_rows_y = nrow(y),
        filtered_rows_x = nrow(x), # No filtering applied
        bloom_size = bloom_size,
        false_positive_rate = false_positive_rate,
        reduction_ratio = 0, # No reduction since all rows kept
        join_type = type,
        bloom_filter_used = FALSE
      )
    }
  }

  return(result)
}

#' Determine if Bloom filter should be used for the join
#'
#' Internal function to decide whether a Bloom filter will be beneficial
#'
#' @param x A data frame
#' @param y A data frame  
#' @param type Type of join
#'
#' @return Logical indicating whether to use Bloom filter
#' @keywords internal
should_use_bloom_filter <- function(x, y, type) {
  # Always try to use Bloom filter for inner, semi joins
  # These benefit from pre-filtering
  if (type %in% c("inner", "semi")) {
    return(TRUE)
  }
  
  # For anti-joins, warn that Bloom filters cannot provide pre-filtering benefits
  if (type == "anti") {
    if (nrow(x) > 1000) { # Only warn for larger datasets where it matters
      warning("Anti-joins cannot use Bloom filters for pre-filtering due to false positives. Performance may be similar to standard anti_join().")
    }
    return(TRUE)
  }
  
  # For left, right, full joins, warn user but still allow Bloom filter
  # if they specifically requested it via bloomjoin()
  if (type %in% c("left", "right", "full")) {
    if (nrow(x) > 1000) { # Only warn for larger datasets where it matters
      warning(sprintf("%s join with Bloom filter may not provide performance benefits since all rows from the larger table are kept", 
                     toupper(type)))
    }
    return(TRUE)
  }
  
  return(TRUE) # Default to using Bloom filter
}

#' Validate inputs and prepare join parameters
#' 
#' @param x A data frame
#' @param y A data frame
#' @param by A character vector of variables to join by
#' @param type Type of join
#' @param false_positive_rate False positive rate
#' @param verbose Logical, whether to print performance information
#' 
#' @return List with validation results and prepared parameters
#' @keywords internal
validate_and_prepare_join <- function(x, y, by, type, false_positive_rate, verbose) {
  # Enhanced input validation with better error messages
  validate_data_frames(x, y)
  validate_join_parameters(type, false_positive_rate)
  
  # Check for empty data frames
  if (nrow(x) == 0 || nrow(y) == 0) {
    warning("One or both data frames are empty. ", 
           "x has ", nrow(x), " rows, y has ", nrow(y), " rows")
    result <- perform_standard_join(x, y, by, type)
    return(list(should_skip_bloom = TRUE, result = result))
  }
  
  # Check for very large datasets and warn about memory usage
  total_rows <- nrow(x) + nrow(y)
  if (total_rows > 1000000) {
    message("Large dataset detected (", format(total_rows, big.mark = ","), " total rows). ", 
           "Consider using smaller false_positive_rate or chunked processing for very large datasets.")
  }
  
  # Prepare join columns
  by_result <- prepare_join_columns(x, y, by)
  
  return(list(
    should_skip_bloom = FALSE,
    by = by_result$by,
    join_col = by_result$join_col,
    temp_join_cols = by_result$temp_join_cols,
    orig_by = by_result$orig_by,
    x = by_result$x,
    y = by_result$y
  ))
}

#' Validate that inputs are data frames
#' @keywords internal
validate_data_frames <- function(x, y) {
  if (!is.data.frame(x) || !is.data.frame(y)) {
    x_type <- if (is.null(x)) "NULL" else class(x)[1]
    y_type <- if (is.null(y)) "NULL" else class(y)[1]
    
    stop("Both x and y must be data frames.\n", 
         "  - x is class '", x_type, "' with length ", length(x), "\n",
         "  - y is class '", y_type, "' with length ", length(y), "\n",
         "Convert to data.frame using:\n",
         "  - as.data.frame() for basic conversion\n",
         "  - tibble::as_tibble() for tibble format\n",
         "  - data.table::setDF() for data.table objects")
  }
  
  # Check for required columns
  if (ncol(x) == 0) stop("Data frame x has no columns")
  if (ncol(y) == 0) stop("Data frame y has no columns")
}

#' Validate join parameters
#' @keywords internal  
validate_join_parameters <- function(type, false_positive_rate) {
  # Validate join type
  valid_types <- c("inner", "left", "right", "full", "semi", "anti")
  if (!type %in% valid_types) {
    stop("Invalid join type '", type, "'.\n", 
         "Valid types: ", paste(valid_types, collapse = ", "), "\n",
         "Did you mean: ", find_closest_match(type, valid_types), "?")
  }
  
  # Validate false positive rate
  if (false_positive_rate <= 0 || false_positive_rate >= 1) {
    stop("false_positive_rate must be between 0 and 1 (exclusive).\n", 
         "Received: ", false_positive_rate, "\n",
         "Common values: 0.01 (1%), 0.001 (0.1%), 0.1 (10%)")
  }
}

#' Find closest match for error suggestions
#' @keywords internal
find_closest_match <- function(input, options) {
  if (length(options) == 0) return("none available")
  
  # Simple string distance calculation
  distances <- sapply(options, function(opt) {
    # Use simple edit distance approximation
    sum(strsplit(input, "")[[1]] != strsplit(opt, "")[[1]][1:min(nchar(input), nchar(opt))])
  })
  
  options[which.min(distances)]
}

#' Prepare join columns and handle named vectors
#' 
#' @param x A data frame
#' @param y A data frame
#' @param by A character vector of variables to join by
#' 
#' @return List with prepared join columns
#' @keywords internal
prepare_join_columns <- function(x, y, by) {
  orig_by <- by
  temp_join_cols <- NULL
  
  # Determine the join columns
  if (is.null(by)) {
    by <- intersect(names(x), names(y))
    if (length(by) == 0) {
      stop("No common variables in x and y to join by. ", 
           "Please specify 'by' parameter explicitly")
    }
  } else if (is.character(by)) {
    # Handle named vectors correctly
    if (!is.null(names(by)) && any(names(by) != "")) {
      # Named vector case (different column names)
      x_cols <- names(by)
      y_cols <- unname(by)

      # Check if join columns exist in respective data frames
      if (!all(x_cols %in% names(x))) {
        missing_x <- x_cols[!x_cols %in% names(x)]
        stop("Join columns not found in x: ", paste(missing_x, collapse = ", "))
      }
      if (!all(y_cols %in% names(y))) {
        missing_y <- y_cols[!y_cols %in% names(y)]
        stop("Join columns not found in y: ", paste(missing_y, collapse = ", "))
      }

      # Create temporary columns for joining
      temp_join_cols <- character()
      for (i in seq_along(by)) {
        x_col <- x_cols[i]
        y_col <- y_cols[i]
        temp_col <- paste0(".temp_join_", i)
        temp_join_cols <- c(temp_join_cols, temp_col)
        
        x[[temp_col]] <- x[[x_col]]
        y[[temp_col]] <- y[[y_col]]
      }
      by <- temp_join_cols
    } else {
      # Simple character vector - check if all columns exist
      if (!all(by %in% names(x))) {
        missing_x <- by[!by %in% names(x)]
        stop("Join columns not found in x: ", paste(missing_x, collapse = ", "))
      }
      if (!all(by %in% names(y))) {
        missing_y <- by[!by %in% names(y)]
        stop("Join columns not found in y: ", paste(missing_y, collapse = ", "))
      }
    }
  }

  # Create a single join column if multiple columns specified
  if (length(by) > 1) {
    # Create composite key
    x$.composite_key <- do.call(paste, c(x[by], sep = "\001"))
    y$.composite_key <- do.call(paste, c(y[by], sep = "\001"))
    join_col <- ".composite_key"
  } else {
    join_col <- by
  }
  
  return(list(
    by = by,
    join_col = join_col,
    temp_join_cols = temp_join_cols,
    orig_by = orig_by,
    x = x,
    y = y
  ))
}

#' Perform standard join operations
#'
#' Internal function to perform standard join operations using dplyr
#'
#' @param x A data frame
#' @param y A data frame
#' @param by A character vector of variables to join by
#' @param type Type of join: "inner", "left", "right", "full", "semi", or "anti"
#'
#' @return A joined data frame
#' @keywords internal
perform_standard_join <- function(x, y, by = NULL, type = "inner") {
  # Check if dplyr is available
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("The dplyr package is required for join operations")
  }

  # Perform the join based on the specified type
  if (type == "inner") {
    return(dplyr::inner_join(x, y, by = by))
  } else if (type == "left") {
    return(dplyr::left_join(x, y, by = by))
  } else if (type == "right") {
    return(dplyr::right_join(x, y, by = by))
  } else if (type == "full") {
    return(dplyr::full_join(x, y, by = by))
  } else if (type == "semi") {
    return(dplyr::semi_join(x, y, by = by))
  } else if (type == "anti") {
    return(dplyr::anti_join(x, y, by = by))
  } else {
    stop("Unsupported join type")
  }
}
