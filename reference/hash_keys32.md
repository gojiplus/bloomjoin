# Hash composite join keys to 32-bit integers (stable & type-aware)

Produces a deterministic 32-bit integer per row from one or more key
columns. The hash respects column types, canonicalizes `-0` to `+0`,
normalizes strings to UTF-8, and maps all NA/NaN variants to a single
value. Factors are hashed by their level strings so that
factor/character equality works as expected.

## Usage

``` r
hash_keys32(x, by, normalize_strings = TRUE)
```

## Arguments

- x:

  A data.frame or tibble containing the key columns.

- by:

  Character vector of column names forming the key, in order.

- normalize_strings:

  Logical; if `TRUE` (default) strings are normalized to UTF-8 before
  hashing.

## Value

An integer vector of length `nrow(x)` containing deterministic 32-bit
hashes. Values may be negative.

## Details

Prior to hashing, POSIXlt columns are converted to POSIXct. When the
optional bit64 package is installed, `integer64` columns are converted
to their exact decimal string representations so they hash losslessly.
Without bit64, `integer64` inputs fall back to
[`as.character()`](https://rdrr.io/r/base/character.html) with the same
equality semantics.

## Examples

``` r
if (interactive()) {
  df <- data.frame(
    id = c(1L, 2L, NA_integer_),
    ts = as.POSIXct("2020-01-01", tz = "UTC") + 0:2,
    value = c("a", "b", NA_character_),
    stringsAsFactors = FALSE
  )
  hash_keys32(df, by = c("id", "ts", "value"))
}
```
