#' BloomJoin: Efficient data.frame joins using probabilistic prefilters
#'
#' @description
#' `bloom_join()` wraps the standard dplyr join verbs with a probabilistic
#' pre-filter stage implemented in C++ via Rcpp. The filter trims the rows that
#' reach the expensive join when the overlap between the two tables is small,
#' yielding faster joins without changing the final result.
#'
#' @details
#' The exported surface mirrors the dplyr join verbs while adding controls for
#' the pre-filter:
#'
#' * `engine` selects the probabilistic data structure (currently only Bloom
#'   filters are implemented).
#' * `prefilter_side` decides which input will be filtered before calling the
#'   underlying join.
#' * `fpr` configures the target false positive rate for the Bloom filter.
#' * `n_hint` allows callers to pass optional distinct-count hints for the join
#'   keys which helps size the filter without extra scans.
#'
#' The function automatically samples the key columns to estimate distinct
#' counts, chooses which side to pre-filter when `prefilter_side = "auto"`, and
#' falls back to the raw dplyr join when the Bloom filter would not remove enough
#' rows to pay for its construction.
#'
#' @importFrom dplyr inner_join left_join right_join full_join semi_join anti_join
#' @importFrom Rcpp sourceCpp evalCpp
#' @useDynLib bloomjoin, .registration = TRUE
#'
#' @name bloomjoin
"_PACKAGE"

#' Join two data frames using a Bloom filter pre-filter
#'
#' @param x,y Data frames to join.
#' @param by A character vector or named character vector specifying the join
#'   columns, just like the `by` argument in the dplyr join verbs.
#' @param type Type of join to perform. One of `"inner"`, `"left"`, `"right"`,
#'   `"full"`, `"semi"`, or `"anti"`.
#' @param engine Probabilistic pre-filter to use. Currently only `"bloom"` is
#'   implemented; `"auto"` resolves to `"bloom"` and `"fuse"` is reserved for a
#'   future binary-fuse implementation.
#' @param prefilter_side Which input to pre-filter. `"x"` filters the first
#'   table, `"y"` filters the second table, and `"auto"` picks a side based on
#'   join semantics and distinct-count estimates.
#' @param fpr Target false positive rate for the Bloom filter. Must be between
#'   0 and 1.
#' @param n_hint Optional estimated distinct counts for the join keys. Supply a
#'   numeric scalar to use the same hint for both tables or a named list/vector
#'   with entries `x` and/or `y`.
#' @param verbose Logical flag controlling diagnostic output.
#'
#' @return A data frame identical to the corresponding dplyr join with an
#'   additional `"bloomjoin"` class and a `bloom_metadata` attribute describing
#'   the pre-filter decision.
#' @export
#'
#' @examples
#' x <- data.frame(id = 1:100000, value_x = rnorm(100000))
#' y <- data.frame(id = 50001:60000, value_y = rnorm(10000))
#' bloom_join(x, y, by = "id", verbose = TRUE)
bloom_join <- function(
    x,
    y,
    by = NULL,
    type = c("inner", "left", "right", "full", "semi", "anti"),
    engine = c("auto", "bloom", "fuse"),
    prefilter_side = c("auto", "x", "y"),
    fpr = 0.01,
    n_hint = NULL,
    verbose = FALSE
) {
  valid_types <- c("inner", "left", "right", "full", "semi", "anti")
  type_input <- type[1]
  type <- tolower(type_input)
  if (!type %in% valid_types) {
    stop("Invalid join type '", type_input, "'")
  }
  engine <- match.arg(engine)
  prefilter_side <- match.arg(prefilter_side)

  validate_join_inputs(x, y, type, fpr)

  join_info <- resolve_join_columns(x, y, by)
  keys_x <- compute_hashed_keys(x, join_info$x_cols)
  keys_y <- compute_hashed_keys(y, join_info$y_cols)

  hints <- normalize_n_hint(n_hint)
  distinct_x <- estimate_distinct_count(keys_x, hints$x)
  distinct_y <- estimate_distinct_count(keys_y, hints$y)

  plan <- plan_prefilter(
    type = type,
    engine = engine,
    prefilter_side = prefilter_side,
    n_x = nrow(x),
    n_y = nrow(y),
    distinct_x = distinct_x,
    distinct_y = distinct_y,
    fpr = fpr,
    keys_x = keys_x,
    keys_y = keys_y,
    verbose = verbose
  )

  execution <- execute_join_plan(
    plan = plan,
    x = x,
    y = y,
    by_spec = join_info$by_spec,
    type = type,
    keys_x = keys_x,
    keys_y = keys_y,
    fpr = fpr,
    verbose = verbose
  )

  result <- execution$result
  metadata <- execution$metadata

  class(result) <- unique(c("bloomjoin", class(result)))
  attr(result, "bloom_metadata") <- metadata
  result
}

#' Compute Bloom filter parameters
#'
#' @param n Estimated number of elements that will be inserted into the filter.
#' @param p Desired false positive rate. Defaults to 0.01.
#'
#' @return A list with the number of bits (`m`) and hash functions (`k`).
#' @export
#'
#' @examples
#' bloom_params(1e6, p = 0.001)
bloom_params <- function(n, p = 0.01) {
  if (length(n) != 1 || !is.numeric(n) || is.na(n) || n <= 0) {
    stop("'n' must be a positive numeric scalar")
  }
  if (length(p) != 1 || !is.numeric(p) || is.na(p) || p <= 0 || p >= 1) {
    stop("'p' must be a numeric scalar between 0 and 1")
  }

  m <- ceiling(-n * log(p) / (log(2)^2))
  k <- max(1L, as.integer(round((m / n) * log(2))))
  list(m = as.integer(m), k = k)
}

validate_join_inputs <- function(x, y, type, fpr) {
  if (!is.data.frame(x) || !is.data.frame(y)) {
    stop("Both 'x' and 'y' must be data frames")
  }
  if (ncol(x) == 0 || ncol(y) == 0) {
    stop("Both 'x' and 'y' must have at least one column")
  }
  if (fpr <= 0 || fpr >= 1) {
    stop("'fpr' must be strictly between 0 and 1")
  }
  valid_types <- c("inner", "left", "right", "full", "semi", "anti")
  if (!type %in% valid_types) {
    stop("Invalid join type '", type, "'")
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("The dplyr package is required for bloom_join()")
  }
}

resolve_join_columns <- function(x, y, by) {
  orig_by <- by
  if (is.null(by)) {
    common <- intersect(names(x), names(y))
    if (length(common) == 0) {
      stop("No common columns and no 'by' argument supplied")
    }
    x_cols <- common
    y_cols <- common
    by_spec <- common
  } else if (is.character(by)) {
    if (!is.null(names(by)) && any(names(by) != "")) {
      x_cols <- names(by)
      y_cols <- unname(by)
      missing_x <- x_cols[!x_cols %in% names(x)]
      missing_y <- y_cols[!y_cols %in% names(y)]
      if (length(missing_x)) {
        stop("Join columns not found in x: ", paste(missing_x, collapse = ", "))
      }
      if (length(missing_y)) {
        stop("Join columns not found in y: ", paste(missing_y, collapse = ", "))
      }
      by_spec <- by
    } else {
      x_cols <- by
      y_cols <- by
      missing_x <- x_cols[!x_cols %in% names(x)]
      missing_y <- y_cols[!y_cols %in% names(y)]
      if (length(missing_x)) {
        stop("Join columns not found in x: ", paste(missing_x, collapse = ", "))
      }
      if (length(missing_y)) {
        stop("Join columns not found in y: ", paste(missing_y, collapse = ", "))
      }
      by_spec <- by
    }
  } else {
    stop("'by' must be NULL or a character vector")
  }

  list(
    by_spec = by_spec,
    x_cols = x_cols,
    y_cols = y_cols,
    orig_by = orig_by
  )
}

compute_hashed_keys <- function(df, cols) {
  if (length(cols) == 0) {
    return(integer(nrow(df)))
  }
  hash_keys32(df, cols)
}

normalize_n_hint <- function(n_hint) {
  hints <- list(x = NA_real_, y = NA_real_)
  if (is.null(n_hint)) {
    return(hints)
  }
  if (is.list(n_hint)) {
    if (length(n_hint) == 1 && is.null(names(n_hint))) {
      value <- as.numeric(n_hint[[1]])
      hints$x <- value
      hints$y <- value
      return(hints)
    }
    for (nm in intersect(names(n_hint), c("x", "y"))) {
      hints[[nm]] <- as.numeric(n_hint[[nm]])
    }
    return(hints)
  }
  if (is.numeric(n_hint)) {
    if (length(n_hint) == 1 && is.null(names(n_hint))) {
      hints$x <- as.numeric(n_hint)
      hints$y <- as.numeric(n_hint)
    } else if (!is.null(names(n_hint))) {
      for (nm in intersect(names(n_hint), c("x", "y"))) {
        hints[[nm]] <- as.numeric(n_hint[[nm]])
      }
    }
    return(hints)
  }
  stop("n_hint must be NULL, numeric, or a list")
}

estimate_distinct_count <- function(keys, hint = NA_real_, sample_limit = 50000L) {
  if (!is.na(hint)) {
    return(max(0L, as.integer(round(hint))))
  }
  n <- length(keys)
  if (n == 0) {
    return(0L)
  }
  if (n <= sample_limit) {
    return(length(unique(keys)))
  }
  idx <- unique(as.integer(round(seq(1, n, length.out = sample_limit))))
  sample_unique <- length(unique(keys[idx]))
  estimate <- round(sample_unique / length(idx) * n)
  max(0L, as.integer(estimate))
}

estimate_selectivity <- function(probe_keys, build_keys, probe_limit = 5000L, build_limit = 50000L) {
  if (!length(probe_keys) || !length(build_keys)) {
    return(0)
  }
  probe_idx <- if (length(probe_keys) <= probe_limit) {
    seq_along(probe_keys)
  } else {
    unique(as.integer(round(seq(1, length(probe_keys), length.out = probe_limit))))
  }
  build_idx <- if (length(build_keys) <= build_limit) {
    seq_along(build_keys)
  } else {
    unique(as.integer(round(seq(1, length(build_keys), length.out = build_limit))))
  }
  probe_sample <- probe_keys[probe_idx]
  build_unique <- unique(build_keys[build_idx])
  mean(probe_sample %in% build_unique)
}

plan_prefilter <- function(type, engine, prefilter_side, n_x, n_y, distinct_x, distinct_y,
                           fpr, keys_x, keys_y, verbose = FALSE) {
  chosen_engine <- if (engine == "auto") "bloom" else engine
  if (chosen_engine == "fuse") {
    stop("engine = 'fuse' is not implemented yet")
  }

  metadata <- list(
    join_type = type,
    engine = chosen_engine,
    fpr = fpr,
    estimated_distinct_x = distinct_x,
    estimated_distinct_y = distinct_y
  )
  if (prefilter_side %in% c("x", "y")) {
    metadata$requested_prefilter_side <- prefilter_side
  }

  target_info <- choose_prefilter_target(type, prefilter_side, n_x, n_y, distinct_x, distinct_y)
  target <- target_info$target
  if (is.null(target)) {
    metadata$reason <- target_info$reason
    metadata$bloom_filter_used <- FALSE
    return(list(use_prefilter = FALSE, metadata = metadata))
  }
  metadata$chosen_prefilter_side <- target
  if (!is.null(target_info$reason)) {
    metadata$reason <- target_info$reason
  }
  if (isTRUE(target_info$forced)) {
    metadata$override_requested_side <- TRUE
  }

  build_keys <- if (target == "x") keys_y else keys_x
  probe_keys <- if (target == "x") keys_x else keys_y
  build_distinct <- if (target == "x") distinct_y else distinct_x
  probe_n <- if (target == "x") n_x else n_y

  selectivity <- estimate_selectivity(probe_keys, build_keys)
  expected_pass <- selectivity + (1 - selectivity) * fpr
  expected_reduction <- max(0, 1 - expected_pass)

  metadata$estimated_selectivity <- selectivity
  metadata$expected_reduction <- expected_reduction
  metadata$probe_rows <- probe_n

  if (should_skip_prefilter(probe_n, build_distinct, expected_reduction)) {
    metadata$reason <- "prefilter skip heuristic triggered"
    metadata$bloom_filter_used <- FALSE
    return(list(use_prefilter = FALSE, metadata = metadata))
  }

  expected_elements <- max(1L, build_distinct)
  metadata$expected_elements <- expected_elements
  metadata$bloom_filter_used <- TRUE

  list(
    use_prefilter = TRUE,
    target = target,
    expected_elements = expected_elements,
    metadata = metadata
  )
}

choose_prefilter_target <- function(type, prefilter_side, n_x, n_y, distinct_x, distinct_y) {
  enforce_semantics <- function(target, reason, warn = FALSE) {
    if (warn) {
      warning(reason, call. = FALSE)
    }
    list(target = target, reason = reason, forced = warn)
  }

  if (prefilter_side %in% c("x", "y")) {
    if (type == "full") {
      return(enforce_semantics(
        NULL,
        "Full joins retain all rows; ignoring explicit prefilter request",
        warn = TRUE
      ))
    }
    if (type %in% c("left", "semi", "anti") && prefilter_side == "x") {
      return(enforce_semantics(
        "y",
        "prefilter_side = 'x' is incompatible with left/semi/anti joins; using 'y' instead",
        warn = TRUE
      ))
    }
    if (type == "right" && prefilter_side == "y") {
      return(enforce_semantics(
        "x",
        "prefilter_side = 'y' is incompatible with right joins; using 'x' instead",
        warn = TRUE
      ))
    }
    return(list(target = prefilter_side))
  }
  if (type == "full") {
    return(list(target = NULL, reason = "Full joins retain all rows"))
  }
  if (type %in% c("left", "semi", "anti")) {
    return(list(target = "y", reason = "Preserving left-side row semantics"))
  }
  if (type == "right") {
    return(list(target = "x", reason = "Right join retains all rows from 'y'"))
  }
  if (n_x == 0 || n_y == 0) {
    return(list(target = NULL, reason = "One of the inputs has zero rows"))
  }
  density_x <- n_x / max(1, distinct_y)
  density_y <- n_y / max(1, distinct_x)
  if (density_x >= density_y) {
    list(target = "x", reason = "Auto-selected to prefilter 'x'")
  } else {
    list(target = "y", reason = "Auto-selected to prefilter 'y'")
  }
}

should_skip_prefilter <- function(probe_n, build_distinct, expected_reduction) {
  if (probe_n == 0 || build_distinct == 0) {
    return(TRUE)
  }
  if (probe_n < 1024) {
    return(TRUE)
  }
  if (build_distinct < 16) {
    return(TRUE)
  }
  if (expected_reduction <= 0.02) {
    return(TRUE)
  }
  FALSE
}

execute_join_plan <- function(plan, x, y, by_spec, type, keys_x, keys_y, fpr, verbose = FALSE) {
  metadata <- plan$metadata
  if (!isTRUE(plan$use_prefilter)) {
    if (verbose) {
      message("Skipping Bloom pre-filter: ", metadata$reason %||% "heuristic disabled")
    }
    result <- perform_standard_join(x, y, by_spec, type)
    metadata$filtered_rows_x <- 0L
    metadata$filtered_rows_y <- 0L
    metadata$reduction_ratio <- 0
    return(list(result = result, metadata = metadata))
  }

  target <- plan$target
  if (target == "x") {
    keep <- rcpp_filter_keys(keys_y, keys_x, plan$expected_elements, fpr)
    if (verbose) {
      message(sprintf("Prefilter retained %d of %d rows from 'x'", sum(keep), length(keep)))
    }
    filtered_x <- x[keep, , drop = FALSE]
    result <- perform_standard_join(filtered_x, y, by_spec, type)
    metadata$filtered_rows_x <- sum(!keep)
    metadata$filtered_rows_y <- 0L
    metadata$retained_rows <- sum(keep)
    metadata$reduction_ratio <- if (length(keep)) mean(!keep) else 0
  } else {
    keep <- rcpp_filter_keys(keys_x, keys_y, plan$expected_elements, fpr)
    if (verbose) {
      message(sprintf("Prefilter retained %d of %d rows from 'y'", sum(keep), length(keep)))
    }
    filtered_y <- y[keep, , drop = FALSE]
    result <- perform_standard_join(x, filtered_y, by_spec, type)
    metadata$filtered_rows_x <- 0L
    metadata$filtered_rows_y <- sum(!keep)
    metadata$retained_rows <- sum(keep)
    metadata$reduction_ratio <- if (length(keep)) mean(!keep) else 0
  }
  metadata$bloom_filter_used <- TRUE
  list(result = result, metadata = metadata)
}

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

perform_standard_join <- function(x, y, by = NULL, type = "inner") {
  args <- list(x = x, y = y, by = by)
  if (type %in% c("inner", "left", "right", "full")) {
    args$relationship <- "many-to-many"
  }

  switch(
    type,
    inner = do.call(dplyr::inner_join, args),
    left = do.call(dplyr::left_join, args),
    right = do.call(dplyr::right_join, args),
    full = do.call(dplyr::full_join, args),
    semi = dplyr::semi_join(x, y, by = by),
    anti = dplyr::anti_join(x, y, by = by),
    stop("Unsupported join type")
  )
}
