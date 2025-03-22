#' bloomjoin: Efficient data.frame joins using Bloom filters
#'
#' @description
#' This package provides optimized join operations for large data frames using
#' Bloom filters to reduce memory usage and improve performance.
#'
#' @importFrom dplyr inner_join left_join right_join full_join semi_join anti_join
#' @importFrom stats time
#' @importFrom utils head tail
#' @importFrom microbenchmark microbenchmark
#' @importFrom ggplot2 ggplot aes geom_bar theme geom_line theme_minimal element_text scale_fill_manual labs
#' @importFrom rlang .data
#' @useDynLib bloomjoin
#' @importFrom Rcpp evalCpp
#'
#' @keywords internal
"_PACKAGE"

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
#' @param verbose Whether to print progress messages
#'
#' @return A joined data frame with a class of "bloomjoin" and additional attributes
#' @export
#'
#' @examples
#' x <- data.frame(id = 1:100000, value_x = rnorm(100000))
#' y <- data.frame(id = 50001:60000, value_y = rnorm(10000))
#' result <- bloom_join(x, y, by = "id")
bloom_join <- function(x, y, by = NULL, type = "inner",
                       bloom_size = NULL, false_positive_rate = 0.01,
                       verbose = FALSE) {
  # Validate inputs
  if (!is.data.frame(x) || !is.data.frame(y)) {
    stop("Both x and y must be data frames")
  }

  # Determine the join columns
  if (is.null(by)) {
    by <- intersect(names(x), names(y))
    if (length(by) == 0) {
      stop("No common variables in x and y to join by")
    }
    if (verbose) {
      message("Joining by: ", paste(by, collapse = ", "))
    }
  } else if (is.character(by)) {
    # Handle named vectors correctly
    if (!is.null(names(by)) && any(names(by) != "")) {
      # Named vector case (different column names)
      x_cols <- names(by)
      y_cols <- unname(by)

      # Check if join columns exist in respective data frames
      if (!all(x_cols %in% names(x))) {
        stop("Not all join columns found in x")
      }
      if (!all(y_cols %in% names(y))) {
        stop("Not all join columns found in y")
      }

      # Create temporary columns for joining
      for (i in seq_along(by)) {
        x_col <- x_cols[i]
        y_col <- y_cols[i]
        x[[paste0(".temp_join_", i)]] <- x[[x_col]]
        y[[paste0(".temp_join_", i)]] <- y[[y_col]]
      }

      # Set join columns to temporary ones
      temp_join_cols <- paste0(".temp_join_", seq_along(by))
      orig_by <- by  # Save original for final join
      by <- temp_join_cols
    } else {
      # Unnamed vector case (same column names)
      # Check if all join columns exist in both data frames
      if (!all(by %in% names(x))) {
        stop("Not all join columns found in x")
      }
      if (!all(by %in% names(y))) {
        stop("Not all join columns found in y")
      }
    }
  } else {
    stop("'by' must be NULL or a character vector")
  }

  # For multiple join columns, create a composite key
  if (length(by) > 1) {
    x$.composite_key <- do.call(paste, c(lapply(by, function(col) x[[col]]), sep = "___"))
    y$.composite_key <- do.call(paste, c(lapply(by, function(col) y[[col]]), sep = "___"))
    join_col <- ".composite_key"
  } else {
    join_col <- by
  }

  # For types other than inner join, we need to handle differently
  if (type != "inner" && type != "semi") {
    # For left join, we need to keep all rows from x anyway
    if (type == "left" || type == "full") {
      # Use standard join but with improved performance
      # Use the original by if we had named vectors
      result <- perform_standard_join(x, y, if (exists("orig_by")) orig_by else by, type)

      # Remove temporary columns if we created them
      if (exists("temp_join_cols")) {
        for (col in temp_join_cols) {
          if (col %in% names(result)) result[[col]] <- NULL
        }
      }

      # Remove composite keys if we created them
      if (join_col == ".composite_key" && ".composite_key" %in% names(result)) {
        result$.composite_key <- NULL
      }

      return(result)
    }
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
    # Use the cardinality of unique keys in y
    bloom_size <- length(unique(keys_in_y))
  }

  # Call the Rcpp function to create the filter and check keys
  is_in_filter <- rcpp_filter_keys(keys_in_y, keys_to_check, bloom_size, false_positive_rate)

  # Filter the larger data frame
  x_filtered <- x[is_in_filter, , drop = FALSE]

  # If we created a composite key, we need to remove it before the final join
  if (join_col == ".composite_key") {
    x_filtered$.composite_key <- NULL
    y$.composite_key <- NULL
  }

  # Perform the final join
  # Use the original by if we had named vectors
  result <- perform_standard_join(x_filtered, y, if (exists("orig_by")) orig_by else by, type)

  # Remove temporary columns if we created them
  if (exists("temp_join_cols")) {
    for (col in temp_join_cols) {
      if (col %in% names(result)) result[[col]] <- NULL
    }
  }

  return(result)
}
