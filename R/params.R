#' Report the Bloom filter chosen for a target false positive rate
#'
#' Given an expected number of distinct keys `n` and a target false positive rate
#' `p`, report the filter [bloom_join()] will build: total bits `m`, number of
#' hash functions `k`, and the false positive rate that combination achieves.
#'
#' The numbers come from the same C++ routine the filter itself uses, so this
#' cannot describe a filter the package does not build. It used to: it advertised
#' a blocked layout, with a `block_bits` argument and a `blocks` count, that the
#' filter has never implemented, and reported an `m` and `k` the filter did not
#' use.
#'
#' Sizing is the standard one (Broder & Mitzenmacher), as used by Guava's
#' `BloomFilter.create` and Spark's:
#'   bits_per_key = -log(p) / (log(2)^2)
#'   k            = log(2) * (m / n)
#'   fpr(m, n, k) = (1 - exp(-k * n / m))^k
#'
#' `m` is then rounded up to a power of two, because the filter indexes with a
#' bit mask rather than a modulo, and `k` is taken from the rounded `m`. Rounding
#' only ever adds bits, so the achieved rate lands at or under the request.
#'
#' @param n Numeric scalar (> 0): expected number of distinct keys to insert.
#'          Fractional values are allowed (estimates).
#' @param p Numeric scalar in (0, 1): target false positive rate.
#'
#' @return A list with class "bloom_params" containing:
#'   - n: input n
#'   - p_target: target p
#'   - m_bits: total bits, a power of two
#'   - bytes: total bytes
#'   - bits_per_key: m_bits / n
#'   - k: number of hash functions
#'   - fpr_est: the rate that (m, n, k) achieves
#' @examples
#' bp <- bloom_params(1e6, 1e-2)
#' bp
#' @export
bloom_params <- function(n, p = 1e-2) {
  if (!is.numeric(n) || length(n) != 1L || !is.finite(n) || n <= 0) {
    stop("`n` must be a positive finite numeric scalar.")
  }
  if (!is.numeric(p) || length(p) != 1L || !is.finite(p) || p <= 0 || p >= 1) {
    stop("`p` must be a numeric scalar strictly between 0 and 1.")
  }

  sizing <- rcpp_bloom_sizing(as.numeric(n), as.numeric(p))
  m_bits <- sizing$m_bits
  k <- as.integer(sizing$k)

  out <- list(
    n            = as.numeric(n),
    p_target     = as.numeric(p),
    m_bits       = m_bits,
    bytes        = as.numeric(m_bits / 8),
    bits_per_key = as.numeric(m_bits / n),
    k            = k,
    fpr_est      = as.numeric((1 - exp(-k * n / m_bits))^k)
  )
  class(out) <- c("bloom_params", "list")

  # The array is capped, so an extreme combination of n and p can be
  # unsatisfiable: n = 1e12 at p = 1e-9 would need about 5 TB. Say so rather
  # than reporting a rate that quietly misses the request.
  if (out$fpr_est > out$p_target) {
    warning(
      "Cannot reach the requested false positive rate of ", signif(p, 3),
      " for n = ", format(n, scientific = FALSE),
      ": the filter is capped at ", format(m_bits, scientific = FALSE),
      " bits, which achieves ", signif(out$fpr_est, 3), ".",
      call. = FALSE
    )
  }

  out
}

#' @export
print.bloom_params <- function(x, ...) {
  fmt_num <- function(v) format(v, big.mark = ",", scientific = FALSE, trim = TRUE)
  cat("Bloom filter for a target false positive rate\n")
  cat("  n (expected keys): ", fmt_num(x$n), "\n", sep = "")
  cat("  target FPR:        ", signif(x$p_target, 3), "\n", sep = "")
  cat("  total bits (m):    ", fmt_num(x$m_bits), "\n", sep = "")
  cat("  total bytes:       ", fmt_num(x$bytes), " (", signif(x$bytes / 1024^2, 3), " MiB)\n", sep = "")
  cat("  bits per key:      ", signif(x$bits_per_key, 4), "\n", sep = "")
  cat("  hashes (k):        ", x$k, "\n", sep = "")
  cat("  achieved FPR:      ", signif(x$fpr_est, 4), "\n", sep = "")
  invisible(x)
}
