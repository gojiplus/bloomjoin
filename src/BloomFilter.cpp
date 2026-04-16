#include "BloomFilter.h"
// [[Rcpp::plugins(cpp11)]]

#include <Rmath.h>
#include <cmath>
#include <cstdint>

// BloomFilter constructor implementation
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : has_na(false) {
  if (expected_elements == 0) expected_elements = 1;
  if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0) {
    false_positive_rate = 0.01;
  }

  // Optimized filter size calculation
  if (false_positive_rate >= 0.1) {
    m = expected_elements * 5;
    k = 2;
  } else if (false_positive_rate >= 0.01) {
    m = expected_elements * 10;
    k = 3;
  } else {
    m = expected_elements * 15;
    k = 4;
  }

  // Ensure power of 2 for fast modulo (using bit masking)
  size_t power = 1;
  while (power < m) power <<= 1;
  m = power;

  // Limit hash functions for speed
  k = std::max<size_t>(1, std::min<size_t>(k, 4));

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

// Column-major composite key hashing
// Optimization 5: Process each column fully before moving to next
namespace {

inline uint64_t splitmix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

inline uint64_t rotl64(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

inline uint64_t mix_in(uint64_t acc, uint64_t h) {
  acc ^= rotl64(h, 23);
  acc *= 0x9e3779b97f4a7c15ULL;
  return acc;
}

inline uint64_t fnv1a64(const unsigned char* data, size_t len) {
  uint64_t h = 1469598103934665603ULL;
  const uint64_t p = 1099511628211ULL;
  for (size_t i = 0; i < len; ++i) {
    h ^= (uint64_t)data[i];
    h *= p;
  }
  return h;
}

inline std::pair<uint64_t, bool> double_bits_canonical(double d) {
  if (ISNAN(d)) {
    return std::make_pair(0xD1B54A32D192ED03ULL, true);
  }
  if (d == 0.0) {
    d = 0.0;
  }
  uint64_t bits = 0;
  std::memcpy(&bits, &d, sizeof(double));
  return std::make_pair(bits, false);
}

static constexpr uint64_t TAG_INT    = 0x9ae16a3b2f90404fULL;
static constexpr uint64_t TAG_DBL    = 0xc949d7c7509e6557ULL;
static constexpr uint64_t TAG_CHR    = 0x8a5cd789635d2dffULL;
static constexpr uint64_t TAG_LGL    = 0x85ebca6b27d4eb4fULL;
static constexpr uint64_t TAG_DATE   = 0x94d049bb133111ebULL;
static constexpr uint64_t TAG_POSIX  = 0xbf58476d1ce4e5b9ULL;

} // namespace

// [[Rcpp::export]]
CharacterVector rcpp_hash_join_keys(List columns) {
  if (columns.size() == 0) {
    stop("No columns supplied for hashing");
  }

  int n_rows = Rf_length(columns[0]);
  std::vector<uint64_t> hashes(n_rows, 1469598103934665603ULL);

  // Column-major processing: process each column fully before moving to next
  for (int col_idx = 0; col_idx < columns.size(); ++col_idx) {
    SEXP column = columns[col_idx];

    // Handle factors by converting to character
    if (Rf_isFactor(column)) {
      column = Rf_coerceVector(column, STRSXP);
    }

    if (Rf_length(column) != n_rows) {
      stop("All join columns must have the same number of rows");
    }

    int col_type = TYPEOF(column);

    switch (col_type) {
    case INTSXP: {
      const int* data = INTEGER(column);
      for (int row = 0; row < n_rows; ++row) {
        int v = data[row];
        uint64_t hv;
        if (v == NA_INTEGER) {
          hv = mix_in(TAG_INT, 0xD1B54A32D192ED03ULL);
        } else {
          hv = mix_in(TAG_INT, splitmix64(static_cast<uint64_t>(static_cast<uint32_t>(v))));
        }
        hashes[row] = mix_in(hashes[row], hv);
      }
      break;
    }
    case LGLSXP: {
      const int* data = LOGICAL(column);
      for (int row = 0; row < n_rows; ++row) {
        int v = data[row];
        uint64_t code;
        if (v == NA_LOGICAL) code = 2u;
        else if (v == 0) code = 0u;
        else code = 1u;
        uint64_t hv = mix_in(TAG_LGL, splitmix64(code));
        hashes[row] = mix_in(hashes[row], hv);
      }
      break;
    }
    case REALSXP: {
      const double* data = REAL(column);
      bool is_date = Rf_inherits(column, "Date");
      bool is_posix = Rf_inherits(column, "POSIXct");
      uint64_t tag = is_date ? TAG_DATE : (is_posix ? TAG_POSIX : TAG_DBL);

      for (int row = 0; row < n_rows; ++row) {
        auto bits_na = double_bits_canonical(data[row]);
        uint64_t hv = mix_in(tag, splitmix64(bits_na.first));
        hashes[row] = mix_in(hashes[row], hv);
      }
      break;
    }
    case STRSXP: {
      for (int row = 0; row < n_rows; ++row) {
        SEXP s = STRING_ELT(column, row);
        uint64_t hv;
        if (s == NA_STRING) {
          hv = mix_in(TAG_CHR, 0xD1B54A32D192ED03ULL);
        } else {
          const char* bytes = Rf_translateCharUTF8(s);
          uint64_t fhv = fnv1a64(reinterpret_cast<const unsigned char*>(bytes), std::strlen(bytes));
          hv = mix_in(TAG_CHR, splitmix64(fhv));
        }
        hashes[row] = mix_in(hashes[row], hv);
      }
      break;
    }
    default: {
      // Fallback for other types: coerce to character
      SEXP as_char = PROTECT(Rf_coerceVector(column, STRSXP));
      for (int row = 0; row < n_rows; ++row) {
        SEXP s = STRING_ELT(as_char, row);
        uint64_t hv;
        if (s == NA_STRING) {
          hv = mix_in(TAG_CHR, 0xD1B54A32D192ED03ULL);
        } else {
          const char* bytes = Rf_translateCharUTF8(s);
          uint64_t fhv = fnv1a64(reinterpret_cast<const unsigned char*>(bytes), std::strlen(bytes));
          hv = mix_in(TAG_CHR, splitmix64(fhv));
        }
        hashes[row] = mix_in(hashes[row], hv);
      }
      UNPROTECT(1);
      break;
    }
    }
  }

  CharacterVector out(n_rows);
  char buffer[17];
  for (int i = 0; i < n_rows; ++i) {
    std::snprintf(buffer, sizeof(buffer), "%016llx", static_cast<unsigned long long>(hashes[i]));
    out[i] = Rf_mkCharCE(buffer, CE_UTF8);
  }

  return out;
}
