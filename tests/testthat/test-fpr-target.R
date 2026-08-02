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
