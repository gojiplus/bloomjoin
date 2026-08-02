#include "BloomFilter.h"
// [[Rcpp::plugins(cpp11)]]

#include <Rmath.h>
#include <cmath>
#include <cstdint>

// BloomFilter constructor implementation
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : has_na(false) {
  // Sizing lives in bloom_sizing() in the header so bloom_params() reports
  // exactly the filter this constructor builds. It used to be a three-way
  // switch on false_positive_rate here, so every rate in [0.01, 0.1) built an
  // identical filter and a request for 1e-9 delivered about 9e-4.
  bloom_sizing(expected_elements, false_positive_rate, &m, &k);

  // Initialize the bit array
  bits.resize(m);

  // Generate seeds
  primary_seed = 0x9e3779b9;
  secondary_seed = 0x85ebca6b;
}

// Add an integer to the filter using double hashing
// Kirsch-Mitzenmacher: compute h1, h2 once, derive k positions
void BloomFilter::add_int(int32_t key) {
  uint32_t h1 = hash_int32(key, primary_seed);
  uint32_t h2 = hash_int32_secondary(key, secondary_seed);
  size_t mask = m - 1;

  for (size_t i = 0; i < k; ++i) {
    size_t pos = (h1 + i * h2) & mask;
    bits.set(pos);
  }
}

// Check if an integer might be in the filter
bool BloomFilter::contains_int(int32_t key) const {
  uint32_t h1 = hash_int32(key, primary_seed);
  uint32_t h2 = hash_int32_secondary(key, secondary_seed);
  size_t mask = m - 1;

  for (size_t i = 0; i < k; ++i) {
    size_t pos = (h1 + i * h2) & mask;
    if (!bits.test(pos)) {
      return false;
    }
  }
  return true;
}

// Batch add integers with prefetch
void BloomFilter::add_int_batch(const int* keys, size_t n) {
  size_t mask = m - 1;
  size_t i = 0;

  // Process 4 at a time with prefetch
  for (; i + 4 <= n; i += 4) {
    // Prefetch upcoming keys
    if (i + 16 < n) {
      __builtin_prefetch(&keys[i + 16], 0, 0);
    }

    // Process 4 keys
    for (size_t j = 0; j < 4; ++j) {
      int32_t key = keys[i + j];
      uint32_t h1 = hash_int32(key, primary_seed);
      uint32_t h2 = hash_int32_secondary(key, secondary_seed);

      for (size_t ki = 0; ki < k; ++ki) {
        size_t pos = (h1 + ki * h2) & mask;
        bits.set(pos);
      }
    }
  }

  // Handle remainder
  for (; i < n; ++i) {
    add_int(keys[i]);
  }
}

// Optimized batch filtering using integer keys directly
// [[Rcpp::export]]
LogicalVector rcpp_filter_keys(IntegerVector y_keys, IntegerVector x_keys,
                               size_t expected_elements, double false_positive_rate = 0.01) {
  R_xlen_t n_y = y_keys.size();
  R_xlen_t n_x = x_keys.size();

  if (n_y == 0) {
    return LogicalVector(n_x, false);
  }

  // Check for NA in y_keys (do once)
  bool has_na_in_y = false;
  const int* y_ptr = INTEGER(y_keys);
  for (R_xlen_t j = 0; j < n_y && !has_na_in_y; ++j) {
    has_na_in_y = (y_ptr[j] == NA_INTEGER);
  }

  // Count unique elements for better filter sizing
  std::vector<int32_t> unique_keys;
  unique_keys.reserve(n_y);
  for (R_xlen_t j = 0; j < n_y; ++j) {
    if (y_ptr[j] != NA_INTEGER) {
      unique_keys.push_back(y_ptr[j]);
    }
  }

  // Sort and count unique
  std::sort(unique_keys.begin(), unique_keys.end());
  auto last = std::unique(unique_keys.begin(), unique_keys.end());
  size_t actual_unique = std::distance(unique_keys.begin(), last);

  if (expected_elements == 0 || actual_unique < expected_elements) {
    expected_elements = std::max(actual_unique, static_cast<size_t>(1));
  }

  // Create filter
  BloomFilter filter(expected_elements, false_positive_rate);
  if (has_na_in_y) {
    filter.add_na();
  }

  // Add unique keys to filter
  for (auto it = unique_keys.begin(); it != last; ++it) {
    filter.add_int(*it);
  }

  // Check all x_keys against the filter
  LogicalVector result(n_x);
  int* result_ptr = LOGICAL(result);
  const int* x_ptr = INTEGER(x_keys);

  R_xlen_t i = 0;

  // Process 4 at a time with prefetch
  for (; i + 4 <= n_x; i += 4) {
    if (i + 16 < n_x) {
      __builtin_prefetch(&x_ptr[i + 16], 0, 0);
    }

    for (R_xlen_t j = 0; j < 4; ++j) {
      int32_t key = x_ptr[i + j];
      if (key == NA_INTEGER) {
        result_ptr[i + j] = has_na_in_y ? TRUE : FALSE;
      } else {
        result_ptr[i + j] = filter.contains_int(key) ? TRUE : FALSE;
      }
    }
  }

  // Handle remainder
  for (; i < n_x; ++i) {
    int32_t key = x_ptr[i];
    if (key == NA_INTEGER) {
      result_ptr[i] = has_na_in_y ? TRUE : FALSE;
    } else {
      result_ptr[i] = filter.contains_int(key) ? TRUE : FALSE;
    }
  }

  return result;
}


// Sizing for a target rate, as the filter itself computes it. bloom_params()
// calls this so the documented helper cannot drift from the implementation.
// [[Rcpp::export]]
List rcpp_bloom_sizing(double n, double false_positive_rate) {
  size_t en = 1;
  if (n >= 1.0) {
    en = (n > 9e15) ? static_cast<size_t>(9e15) : static_cast<size_t>(std::ceil(n));
  }
  size_t m_bits = 0, k = 0;
  bloom_sizing(en, false_positive_rate, &m_bits, &k);
  return List::create(_["m_bits"] = static_cast<double>(m_bits),
                      _["k"] = static_cast<int>(k));
}
