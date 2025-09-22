test_that("bloom_params computes rounded parameters", {
  bp <- bloom_params(1e6, 1e-2)

  expect_s3_class(bp, "bloom_params")
  expect_equal(bp$n, 1e6)
  expect_equal(bp$p_target, 1e-2)
  expect_equal(bp$block_bits, 512L)
  expect_equal(bp$m_bits, 9585152L)
  expect_equal(bp$bytes, bp$m_bits / 8)
  expect_equal(bp$bits_per_key, bp$m_bits / bp$n)
  expect_equal(bp$k, 7L)
  expect_equal(bp$blocks, bp$m_bits / bp$block_bits)
  expect_equal(bp$fpr_est, 0.01003875, tolerance = 1e-6)
})

test_that("bloom_params validates inputs", {
  expect_error(bloom_params(0, 0.1), "positive")
  expect_error(bloom_params(10, 1), "between 0 and 1")
  expect_error(bloom_params(10, 0.1, block_bits = 0), "positive integer")
  expect_error(bloom_params(10, 0.1, min_k = 5L, max_k = 4L), "max_k >= min_k")
})
