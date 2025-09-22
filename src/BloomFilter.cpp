#include "BloomFilter.h"
// [[Rcpp::plugins(cpp11)]]

#include <Rmath.h>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>

// MurmurHash3 implementation (improved)
uint32_t MurmurHash3(const std::string& key, uint32_t seed) {
  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;
  const int r1 = 15;
  const int r2 = 13;
  const uint32_t m = 5;
  const uint32_t n = 0xe6546b64;

  uint32_t hash = seed;
  
  // Process key in 4-byte chunks
  const int nblocks = key.size() / 4;
  const uint32_t* blocks = reinterpret_cast<const uint32_t*>(key.c_str());
  
  for (int i = 0; i < nblocks; i++) {
    uint32_t k = blocks[i];
    
    k *= c1;
    k = (k << r1) | (k >> (32 - r1));
    k *= c2;
    
    hash ^= k;
    hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
  }
  
  // Handle remaining bytes
  const uint8_t* tail = reinterpret_cast<const uint8_t*>(key.c_str() + nblocks * 4);
  uint32_t k = 0;
  
  switch (key.size() & 3) {
    case 3:
      k ^= tail[2] << 16;
    case 2:
      k ^= tail[1] << 8;
    case 1:
      k ^= tail[0];
      k *= c1;
      k = (k << r1) | (k >> (32 - r1));
      k *= c2;
      hash ^= k;
  }
  
  // Finalization
  hash ^= key.size();
  hash ^= (hash >> 16);
  hash *= 0x85ebca6b;
  hash ^= (hash >> 13);
  hash *= 0xc2b2ae35;
  hash ^= (hash >> 16);
  
  return hash;
}

// Fast and simple hash function - optimized for speed
uint32_t FastHash(const std::string& key, uint32_t seed) {
  uint32_t hash = seed;
  const char* data = key.c_str();
  const size_t len = key.size();
  
  // Process 4 bytes at a time when possible
  const size_t blocks = len / 4;
  const uint32_t* blocks32 = reinterpret_cast<const uint32_t*>(data);
  
  for (size_t i = 0; i < blocks; i++) {
    uint32_t k = blocks32[i];
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    hash ^= k;
    hash = ((hash << 13) | (hash >> 19)) * 5 + 0xe6546b64;
  }
  
  // Handle remaining bytes
  const uint8_t* tail = reinterpret_cast<const uint8_t*>(data + blocks * 4);
  uint32_t k = 0;
  switch (len & 3) {
    case 3: k ^= tail[2] << 16;
    case 2: k ^= tail[1] << 8;
    case 1: k ^= tail[0];
            k *= 0xcc9e2d51;
            k = (k << 15) | (k >> 17);
            k *= 0x1b873593;
            hash ^= k;
  }
  
  // Final mix
  hash ^= len;
  hash ^= hash >> 16;
  hash *= 0x85ebca6b;
  hash ^= hash >> 13;
  hash *= 0xc2b2ae35;
  hash ^= hash >> 16;
  
  return hash;
}

// Optimized double hashing using faster hash functions and bit masking
uint32_t DoubleHash(const std::string& key, uint32_t seed, uint32_t i, size_t m) {
  uint32_t h1 = FastHash(key, seed);
  uint32_t h2 = FastHash(key, seed + 1) | 1; // Ensure h2 is odd
  // Use bit masking instead of modulo since m is power of 2
  return (h1 + i * h2) & (m - 1);
}

inline std::string encode_int32(int value) {
  uint32_t v = static_cast<uint32_t>(value);
  char buf[4];
  buf[0] = static_cast<char>((v >> 24) & 0xFF);
  buf[1] = static_cast<char>((v >> 16) & 0xFF);
  buf[2] = static_cast<char>((v >> 8) & 0xFF);
  buf[3] = static_cast<char>(v & 0xFF);
  return std::string(buf, 4);
}


// Check if key is NA-like
bool BloomFilter::is_na_like(const std::string& key) {
  return key == "NA" || key == "" || key == "NULL" || key == "<NA>" || key == "NaN";
}

// BloomFilter constructor implementation
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate) {
  // Validate inputs
  if (expected_elements == 0) expected_elements = 1;
  if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0) {
    false_positive_rate = 0.01;
  }
  
  // Optimized filter size calculation - favor speed over perfect optimization
  if (false_positive_rate >= 0.1) {
    // High FPR - use smaller filter
    m = expected_elements * 5;
    k = 2;
  } else if (false_positive_rate >= 0.01) {
    // Standard FPR
    m = expected_elements * 10;
    k = 3;
  } else {
    // Low FPR - larger filter
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
  bits.resize(m, false);

  // Generate high-quality seeds
  primary_seed = 0x9e3779b9; // Golden ratio based seed
  secondary_seed = 0x85ebca6b; // Different constant
}

// Add an element to the filter
void BloomFilter::add(const std::string& key) {
  // Handle NA-like values separately
  if (is_na_like(key)) {
    na_values.insert(key);
    return;
  }
  
  for (size_t i = 0; i < k; ++i) {
    uint32_t hash = DoubleHash(key, primary_seed, i, m);
    bits[hash] = true;
  }
}

// Add multiple elements efficiently
void BloomFilter::add_batch(const std::vector<std::string>& keys) {
  for (const auto& key : keys) {
    add(key);
  }
}

// Check if an element might be in the filter
bool BloomFilter::contains(const std::string& key) const {
  // Handle NA-like values separately
  if (is_na_like(key)) {
    return na_values.count(key) > 0;
  }
  
  for (size_t i = 0; i < k; ++i) {
    uint32_t hash = DoubleHash(key, primary_seed, i, m);
    if (!bits[hash]) {
      return false;  // Definitely not in the set
    }
  }
  return true;  // Might be in the set
}

// Get the size of the filter
size_t BloomFilter::size() const {
  return m;
}

// Get the number of hash functions
size_t BloomFilter::num_hashes() const {
  return k;
}

// Get actual number of bits set (for debugging)
size_t BloomFilter::bits_set() const {
  return std::count(bits.begin(), bits.end(), true);
}

// Rcpp module to expose BloomFilter to R
RCPP_MODULE(blm_module) {
  class_<BloomFilter>("BloomFilter")
  .constructor<size_t, double>()
  .method("add", &BloomFilter::add)
  .method("contains", &BloomFilter::contains)
  .method("size", &BloomFilter::size)
  .method("num_hashes", &BloomFilter::num_hashes)
  ;
}

// Function to create a filter and add keys efficiently
XPtr<BloomFilter> rcpp_create_filter(IntegerVector keys, size_t expected_elements,
                                     double false_positive_rate = 0.01) {
  // Calculate unique elements for better sizing
  std::unordered_set<std::string> unique_keys;
  for (int i = 0; i < keys.size(); i++) {
    if (!IntegerVector::is_na(keys[i])) {
      unique_keys.insert(encode_int32(keys[i]));
    }
  }
  
  // Use actual unique count for better filter sizing
  size_t actual_elements = std::max(unique_keys.size(), static_cast<size_t>(1));
  if (expected_elements == 0 || actual_elements < expected_elements) {
    expected_elements = actual_elements;
  }
  
  // Create filter
  XPtr<BloomFilter> filter(new BloomFilter(expected_elements, false_positive_rate), true);

  // Add all keys (including duplicates for completeness)
  for (int i = 0; i < keys.size(); i++) {
    if (!IntegerVector::is_na(keys[i])) {
      std::string key = encode_int32(keys[i]);
      filter->add(key);
    }
  }

  return filter;
}

// Function to check keys against the filter
LogicalVector rcpp_check_keys(XPtr<BloomFilter> filter, IntegerVector keys) {
  LogicalVector result(keys.size());

  for (int i = 0; i < keys.size(); i++) {
    std::string key = encode_int32(keys[i]);
    result[i] = filter->contains(key);
  }

  return result;
}

// Optimized batch processing function
// [[Rcpp::export]]
LogicalVector rcpp_filter_keys(IntegerVector y_keys, IntegerVector x_keys,
                               size_t expected_elements, double false_positive_rate = 0.01) {
  // Input validation
  if (y_keys.size() == 0) {
    return LogicalVector(x_keys.size(), false);
  }
  
  // Pre-check for NA values in y_keys (do this once)
  bool has_na_in_y = false;
  for (int j = 0; j < y_keys.size() && !has_na_in_y; j++) {
    has_na_in_y = IntegerVector::is_na(y_keys[j]);
  }
  
  // Create the filter from y_keys with optimal sizing
  XPtr<BloomFilter> filter = rcpp_create_filter(y_keys, expected_elements, false_positive_rate);

  // Check all x_keys against the filter - optimized loop
  LogicalVector result(x_keys.size());
  
  for (int i = 0; i < x_keys.size(); i++) {
    if (IntegerVector::is_na(x_keys[i])) {
      result[i] = has_na_in_y;
    } else {
      std::string key = encode_int32(x_keys[i]);
      result[i] = filter->contains(key);
    }
  }

  return result;
}

namespace {

inline uint64_t splitmix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

inline uint64_t combine_hash(uint64_t current, uint64_t value) {
  return splitmix64(current ^ (value + 0x9e3779b97f4a7c15ULL + (current << 6) + (current >> 2)));
}

inline uint64_t hash_string_data(const char* data, size_t length) {
  uint64_t hash = 1469598103934665603ULL; // FNV offset basis
  for (size_t i = 0; i < length; ++i) {
    hash ^= static_cast<unsigned char>(data[i]);
    hash *= 1099511628211ULL; // FNV prime
  }
  return hash;
}

inline uint64_t hash_integer_value(int value) {
  if (value == NA_INTEGER) {
    return 0x9ae16a3b2f90404fULL;
  }
  return splitmix64(static_cast<uint64_t>(static_cast<int64_t>(value)));
}

inline uint64_t hash_logical_value(int value) {
  if (value == NA_LOGICAL) {
    return 0x243f6a8885a308d3ULL;
  }
  return value ? 0x13198a2e03707344ULL : 0xa4093822299f31d0ULL;
}

inline uint64_t hash_double_value(double value) {
  if (ISNA(value)) {
    return 0x3bd39e10cb0ef593ULL;
  }
  if (ISNAN(value)) {
    return 0xc0f2f6c9c2fa1a1bULL;
  }
  if (!R_finite(value)) {
    return value > 0 ? 0x5a827999fcef3247ULL : 0x6ed9eba1e6b9f48dULL;
  }
  if (value == 0.0) {
    return 0ULL;
  }
  union {
    double d;
    uint64_t u;
  } converter;
  converter.d = value;
  return splitmix64(converter.u);
}

inline uint64_t hash_character_value(SEXP element) {
  if (element == NA_STRING) {
    return 0x7f4a7c15bf58476dULL;
  }
  const char* data = Rf_translateCharUTF8(element);
  size_t length = std::strlen(data);
  return hash_string_data(data, length);
}

inline uint64_t hash_scalar(SEXP column, int index) {
  switch (TYPEOF(column)) {
    case INTSXP:
      return hash_integer_value(INTEGER(column)[index]);
    case LGLSXP:
      return hash_logical_value(LOGICAL(column)[index]);
    case REALSXP:
      return hash_double_value(REAL(column)[index]);
    case STRSXP:
      return hash_character_value(STRING_ELT(column, index));
    default:
      break;
  }

  SEXP as_char = Rf_coerceVector(column, STRSXP);
  return hash_character_value(STRING_ELT(as_char, index));
}

inline SEXP maybe_as_character(SEXP column) {
  if (Rf_isFactor(column)) {
    return Rf_coerceVector(column, STRSXP);
  }
  return column;
}

} // namespace

// [[Rcpp::export]]
CharacterVector rcpp_hash_join_keys(List columns) {
  if (columns.size() == 0) {
    stop("No columns supplied for hashing");
  }

  int n_rows = Rf_length(columns[0]);
  std::vector<uint64_t> hashes(n_rows, 1469598103934665603ULL);

  for (int i = 0; i < columns.size(); ++i) {
    SEXP column = columns[i];
    column = maybe_as_character(column);

    if (Rf_length(column) != n_rows) {
      stop("All join columns must have the same number of rows");
    }

    for (int row = 0; row < n_rows; ++row) {
      uint64_t value_hash = hash_scalar(column, row);
      hashes[row] = combine_hash(hashes[row], value_hash);
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
