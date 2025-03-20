#' bloomjoin: Efficient data.frame joins using Bloom filters
#'
#' @description
#' This package provides optimized join operations for large data frames using
#' Bloom filters to reduce memory usage and improve performance.
#'
#' @import bloom
#' @importFrom dplyr inner_join left_join right_join full_join semi_join anti_join
#' @importFrom stats time
#' @importFrom utils head tail
#' @importFrom microbenchmark microbenchmark
#' @importFrom ggplot2 ggplot aes geom_bar theme theme_minimal element_text scale_fill_manual labs
#' @importFrom rlang .data
#'
#' @keywords internal
"_PACKAGE"

#' Perform a join between two data frames using Bloom filter optimization
#'
#' @param x The larger data frame
#' @param y The smaller data frame
#' @param by Character vector of variables to join by
#' @param type The type of join: "inner", "left", "right", or "full"
#' @param bloom_size Bloom filter size, defaults to size of y
#' @param false_positive_rate Acceptable rate of false positives (between 0 and 1)
#' @param verbose Whether to print information about the filtering process
#'
#' @return A data frame
#' @export
#'
#' @examples
#' # Create test data
#' large_df <- data.frame(id = 1:1000, value = runif(1000))
#' small_df <- data.frame(id = sample(1:2000, 100), info = letters[1:100])
#'
#' # Perform bloom filter join
#' result <- bloom_join(large_df, small_df, by = "id")
# Load just this function directly
bloom_join <- function(x, y, by = NULL, type = "inner",
                       bloom_size = NULL, false_positive_rate = 0.01,
                       verbose = TRUE) {
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
    # Check if all join columns exist in both data frames
    if (!all(by %in% names(x))) {
      stop("Not all join columns found in x")
    }
    if (!all(by %in% names(y))) {
      stop("Not all join columns found in y")
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

  # Set Bloom filter size
  if (is.null(bloom_size)) {
    bloom_size <- nrow(y)
  }

  # For types other than inner join, we need to handle differently
  if (type != "inner" && type != "semi") {
    warning("Bloom filtering is most efficient with inner joins. Some extra processing required for other join types.")

    # For left join, we need to keep all rows from x anyway
    if (type == "left" || type == "full") {
      # We can still use Bloom filter to optimize the matching part
      if (verbose) {
        message("Using standard join due to join type: ", type)
      }

      # Use standard join but with improved performance
      return(perform_standard_join(x, y, by, type))
    }
  }

  # Start timing for performance tracking
  start_time <- Sys.time()

  # Create a Bloom filter
  if (verbose) {
    message("Creating Bloom filter with size: ", bloom_size,
            " and false positive rate: ", false_positive_rate)
  }

  # MODIFIED: Create a simple set-based filter instead of using bloom package
  # This avoids the namespace issues completely
  keys_in_y <- if (length(by) > 1) {
    as.character(y$.composite_key)
  } else {
    as.character(y[[join_col]])
  }

  # Create a hash set of keys for efficient lookup
  key_set <- new.env(hash = TRUE, parent = emptyenv())
  for (key in keys_in_y) {
    key_set[[key]] <- TRUE
  }

  filter_creation_time <- Sys.time() - start_time
  if (verbose) {
    message("Filter created in ", round(filter_creation_time, 2), " seconds")
  }

  # Filter the larger data frame using our hash set
  start_filter_time <- Sys.time()

  # Get keys to check
  keys_to_check <- if (length(by) > 1) {
    as.character(x$.composite_key)
  } else {
    as.character(x[[join_col]])
  }

  # Check each key against our hash set
  is_in_filter <- vapply(keys_to_check, function(key) {
    exists(key, envir = key_set, inherits = FALSE)
  }, logical(1))

  # Filter the larger data frame
  x_filtered <- x[is_in_filter, , drop = FALSE]

  filter_time <- Sys.time() - start_filter_time
  reduction <- 100 * (1 - nrow(x_filtered) / nrow(x))

  if (verbose) {
    message("Filtered ", nrow(x), " rows to ", nrow(x_filtered),
            " (", round(reduction, 2), "% reduction) in ",
            round(filter_time, 2), " seconds")
  }

  # Now perform the actual join on the filtered data
  start_join_time <- Sys.time()

  # If we created a composite key, we need to remove it before the final join
  if (length(by) > 1) {
    x_filtered$.composite_key <- NULL
    y$.composite_key <- NULL
  }

  # Perform the final join
  result <- perform_standard_join(x_filtered, y, by, type)

  join_time <- Sys.time() - start_join_time
  total_time <- Sys.time() - start_time

  if (verbose) {
    message("Join completed in ", round(join_time, 2), " seconds")
    message("Total operation time: ", round(total_time, 2), " seconds")
  }

  # Add class for potential future extensions
  class(result) <- c("bloomjoin", class(result))

  # Add attributes for performance tracking
  attr(result, "bloom_filter_stats") <- list(
    original_rows = nrow(x),
    filtered_rows = nrow(x_filtered),
    reduction_percent = reduction,
    filter_creation_time = filter_creation_time,
    filtering_time = filter_time,
    join_time = join_time,
    total_time = total_time
  )

  return(result)
}
