#' Hash composite join keys to 32-bit integers (stable & type-aware)
#'
#' Produces a deterministic 32-bit integer per row from one or more key columns.
#' The hash respects types (int vs dbl vs chr vs factor), canonicalizes -0 to +0,
#' normalizes strings to UTF-8, and maps all NA/NaN variants to a single value.
#' Factors hash by their level strings so that factor/character equality works.
#'
#' @param x A data.frame or tibble.
#' @param by Character vector of column names forming the key (in order).
#' @param normalize_strings Logical; if TRUE (default), normalize strings to UTF-8 before hashing.
#' @return An integer vector of length nrow(x) with 32-bit hashes (may be negative).
#' @export
hash_keys32 <- function(x, by, normalize_strings = TRUE) {
  if (!is.data.frame(x)) stop("`x` must be a data.frame or tibble.")
  if (!length(by)) stop("`by` must contain at least one column name.")

  # ensure all columns exist
  missing <- setdiff(by, names(x))
  if (length(missing)) {
    stop("Missing key columns: ", paste(missing, collapse = ", "))
  }

  # Shallow copy subset in order
  cols <- x[by]

  # Normalize awkward classes before handing to C++
  for (nm in by) {
    v <- cols[[nm]]

    # POSIXlt -> POSIXct (numeric seconds)
    if (inherits(v, "POSIXlt") && !inherits(v, "POSIXct")) {
      cols[[nm]] <- as.POSIXct(v)
      next
    }

    # bit64::integer64 -> character (lossless numeric string)
    if (inherits(v, "integer64")) {
      if (requireNamespace("bit64", quietly = TRUE)) {
        # This yields exact decimal strings; hashing strings keeps equality exact
        cols[[nm]] <- bit64::as.character.integer64(v)
      } else {
        # Fallback: as.character() still preserves equality semantics
        # but may be slower; emit a message once.
        cols[[nm]] <- as.character(v)
        packageStartupMessage(
          "Column '", nm, "' is <integer64> but package 'bit64' is not installed; ",
          "hashing via as.character(). Consider installing 'bit64' for speed."
        )
      }
      next
    }

    # Ensure raw vectors are upgraded to hex strings (rare as join keys)
    if (is.raw(v)) {
      cols[[nm]] <- format(v)
      next
    }
  }

  # Call the C++ core on the list of columns (keeps order)
  hash_keys32_cols(cols, normalize_strings = isTRUE(normalize_strings))
}

