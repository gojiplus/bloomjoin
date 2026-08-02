## bloom_params() used to describe a blocked filter, with a block_bits argument
## and a blocks count, that the C++ has never implemented -- and reported an m
## and k the filter did not use. It now calls the filter's own sizing routine,
## so these tests pin the report to the implementation rather than to a formula
## re-derived alongside it.

test_that("bloom_params reports the standard sizing", {
  bp <- bloom_params(1e6, 1e-2)

  expect_s3_class(bp, "bloom_params")
  expect_equal(bp$n, 1e6)
  expect_equal(bp$p_target, 1e-2)
  expect_equal(bp$bytes, bp$m_bits / 8)
  expect_equal(bp$bits_per_key, bp$m_bits / bp$n)

  # m is a power of two, because the filter indexes with a mask, not a modulo.
  expect_equal(log2(bp$m_bits), round(log2(bp$m_bits)))

  # And never fewer bits than the target calls for.
  expect_gte(bp$m_bits, -1e6 * log(1e-2) / log(2)^2)
  expect_gte(bp$k, 1L)
})

test_that("the reported achieved rate meets the target", {
  for (p in c(1e-1, 1e-2, 1e-3, 1e-4, 1e-6)) {
    expect_lte(bloom_params(1e6, p)$fpr_est, p)
  }
})

test_that("bloom_params describes the filter that is actually built", {
  # The defect this replaces: the helper and the filter each computed their own
  # size, and disagreed -- bloom_params said m = 9,585,152 and k = 7 where the
  # filter built a power-of-two m with a k of its own.
  sizing <- getFromNamespace("rcpp_bloom_sizing", "bloomjoin")
  for (n in c(1e4, 1e5, 1e6)) {
    for (p in c(1e-1, 1e-2, 1e-4)) {
      bp <- bloom_params(n, p)
      actual <- sizing(n, p)
      expect_equal(bp$m_bits, actual$m_bits)
      expect_equal(bp$k, as.integer(actual$k))
    }
  }
})

test_that("bloom_params validates inputs", {
  expect_error(bloom_params(0, 0.1), "positive")
  expect_error(bloom_params(-1, 0.1), "positive")
  expect_error(bloom_params(10, 1), "between 0 and 1")
  expect_error(bloom_params(10, 0), "between 0 and 1")
})
