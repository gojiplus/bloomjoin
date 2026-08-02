## `fpr` is documented as "Target false positive rate for the Bloom filter".
##
## The filter used to size itself from a three-way switch on that value -- 5/10/15
## bits per key and k of 2/3/4 -- so every rate in [0.01, 0.1) built an identical
## filter and anything below 0.001 was floored at roughly 9e-4. A request for 1e-6
## was overshot by three orders of magnitude, silently.
##
## Measured against disjoint key sets, so every survivor is a false positive and
## no RNG is involved.

filter_keys <- function(...) getFromNamespace("rcpp_filter_keys", "bloomjoin")(...)

achieved_fpr <- function(p, n = 100000L) {
  build <- seq_len(n)
  probe <- seq.int(n + 1L, 2L * n)
  mean(filter_keys(as.integer(build), as.integer(probe), n, p))
}

test_that("the achieved false positive rate meets the requested one", {
  for (p in c(1e-1, 1e-2, 1e-3, 1e-4, 1e-5)) {
    expect_lte(achieved_fpr(p), p)
  }
})

test_that("a smaller requested rate actually buys a smaller one", {
  # The switch returned bit-identical filters across each of its three bands,
  # so tightening the request within a band changed nothing at all.
  expect_lt(achieved_fpr(1e-4), achieved_fpr(1e-2))
  expect_lt(achieved_fpr(1e-2), achieved_fpr(1e-1))
})

test_that("filtering never drops a key that is present", {
  # The guarantee that makes a Bloom prefilter safe: false positives allowed,
  # false negatives never.
  n <- 100000L
  build <- as.integer(seq_len(n))
  for (p in c(1e-1, 1e-2, 1e-5)) {
    expect_true(all(filter_keys(build, build, n, p)))
  }
})

test_that("the target is met at loose and tight rates, not just typical ones", {
  # The closed form assumes a real-valued k. Rounding to an integer and
  # flooring at 1 used to overshoot: p = 0.8 gave an optimum of k = 0.36, which
  # floored to 1 and achieved 0.85. The sizing now buys bits until the request
  # is met.
  sizing <- getFromNamespace("rcpp_bloom_sizing", "bloomjoin")
  for (case in list(c(1e6, 0.8), c(1e6, 0.5), c(1e6, 0.2), c(10, 1e-20),
                    c(1e5, 1e-4), c(1e6, 1e-6))) {
    n <- case[1]
    p <- case[2]
    z <- sizing(n, p)
    achieved <- (1 - exp(-z$k * n / z$m_bits))^z$k
    expect_lte(achieved, p, label = paste0("n=", n, " p=", p))
  }
})

test_that("an unsatisfiable target warns rather than missing quietly", {
  # 1e12 keys at 1e-9 needs roughly 5 TB; the array is capped well below that.
  expect_warning(bloom_params(1e12, 1e-9), "Cannot reach the requested")
})
