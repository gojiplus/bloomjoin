#' Choose Bloom filter parameters for a blocked layout
#'
#' Given an expected number of distinct keys `n` and target false positive rate `p`,
#' compute total bits `m` (rounded up to a multiple of `block_bits`), number of
#' hash functions `k`, the achieved false positive rate after rounding, and related
#' quantities. Uses the standard Bloom filter formulas:
#'   bits_per_key = -log(p) / (log(2)^2)
#'   k_opt        = log(2) * (m / n)
#'   fpr(m, n, k) = (1 - exp(-k * n / m))^k
#'
#' @param n Numeric scalar (> 0): expected number of distinct keys to insert.
#'          Fractional values are allowed (estimates).
#' @param p Numeric scalar in (0, 1): target false positive rate.
#' @param block_bits Integer (default 512): block size in bits for a blocked Bloom filter.
#'                   512 corresponds to one 64-byte cache line.
#' @param min_k,max_k Integers: bounds for number of hash functions (defaults 1, 12).
#' @param round_to Integer or NULL: round `m` up to a multiple of this many bits.
#'                 Default is `block_bits` (use NULL to disable rounding).
#'
#' @return A list with class "bloom_params" containing:
#'   - n: input n
#'   - p_target: target p
#'   - block_bits: block size used
#'   - m_bits: total bits (rounded)
#'   - bytes: total bytes
#'   - bits_per_key: m_bits / n
#'   - k: chosen number of hash functions
#'   - fpr_est: achieved FPR after rounding
#'   - blocks: number of blocks (m_bits / block_bits)
#' @examples
#' bp <- bloom_params(1e6, 1e-2)
#' bp
#' # Expect k ~ 7, bytes ~ 1.14 MB with default 512-bit blocks.
#' @export
bloom_params <- function(n,
                         p = 1e-2,
                         block_bits = 512L,
                         min_k = 1L,
                         max_k = 12L,
                         round_to = block_bits) {
  # --- validation ---
  if (!is.numeric(n) || length(n) != 1L || !is.finite(n) || n <= 0) {
    stop("`n` must be a positive finite numeric scalar.")
  }
  if (!is.numeric(p) || length(p) != 1L || !is.finite(p) || p <= 0 || p >= 1) {
    stop("`p` must be a numeric scalar strictly between 0 and 1.")
  }
  if (!is.null(round_to)) {
    if (!is.numeric(round_to) || length(round_to) != 1L || round_to <= 0 || round_to != as.integer(round_to)) {
      stop("`round_to` must be a positive integer or NULL.")
    }
  }
  if (!is.numeric(block_bits) || length(block_bits) != 1L ||
      block_bits <= 0 || block_bits != as.integer(block_bits)) {
    stop("`block_bits` must be a positive integer.")
  }
  if (!is.numeric(min_k) || !is.numeric(max_k) ||
      min_k != as.integer(min_k) || max_k != as.integer(max_k) ||
      min_k <= 0 || max_k < min_k) {
    stop("`min_k` and `max_k` must be positive integers with max_k >= min_k.")
  }

  ln2 <- log(2)
  # Ideal bits per key for target p (unrounded)
  bpk_ideal <- -log(p) / (ln2 * ln2)

  # Ideal total bits (unrounded), then ceil to integer bits
  m_raw <- ceiling(bpk_ideal * n)

  # Round up to multiple of `round_to` bits (defaults to block_bits)
  if (is.null(round_to)) {
    m_bits <- as.integer(m_raw)
  } else {
    m_bits <- as.integer(ceiling(m_raw / round_to) * round_to)
  }

  # k near-optimal for the *rounded* m
  k <- as.integer(round((m_bits / n) * ln2))
  if (k < min_k) k <- min_k
  if (k > max_k) k <- max_k

  # Achieved FPR given (m, n, k)
  fpr_est <- (1 - exp(-k * n / m_bits))^k

  out <- list(
    n            = as.numeric(n),
    p_target     = as.numeric(p),
    block_bits   = as.integer(block_bits),
    m_bits       = as.integer(m_bits),
    bytes        = as.numeric(m_bits / 8),
    bits_per_key = as.numeric(m_bits / n),
    k            = as.integer(k),
    fpr_est      = as.numeric(fpr_est),
    blocks       = as.integer(ceiling(m_bits / block_bits))
  )
  class(out) <- c("bloom_params", "list")
  out
}

#' @export
print.bloom_params <- function(x, ...) {
  fmt_num <- function(v) format(v, big.mark = ",", scientific = FALSE, trim = TRUE)
  cat("Bloom parameters (blocked)\n")
  cat("  n (expected keys): ", fmt_num(x$n), "\n", sep = "")
  cat("  target FPR:        ", signif(x$p_target, 3), "\n", sep = "")
  cat("  block bits:        ", x$block_bits, "\n", sep = "")
  cat("  total bits (m):    ", fmt_num(x$m_bits), "\n", sep = "")
  cat("  total bytes:       ", fmt_num(x$bytes), " (", signif(x$bytes / 1024^2, 3), " MiB)\n", sep = "")
  cat("  bits per key:      ", signif(x$bits_per_key, 4), "\n", sep = "")
  cat("  hashes (k):        ", x$k, "\n", sep = "")
  cat("  achieved FPR:      ", signif(x$fpr_est, 4), "\n", sep = "")
  cat("  blocks:            ", x$blocks, "\n", sep = "")
  invisible(x)
}
