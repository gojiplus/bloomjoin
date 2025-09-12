#include "BloomFilter.h"
// [[Rcpp::plugins(cpp11)]]

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
// [[Rcpp::export]]
XPtr<BloomFilter> rcpp_create_filter(CharacterVector keys, size_t expected_elements,
                                     double false_positive_rate = 0.01) {
  // Calculate unique elements for better sizing
  std::unordered_set<std::string> unique_keys;
  for (int i = 0; i < keys.size(); i++) {
    if (!CharacterVector::is_na(keys[i])) {
      unique_keys.insert(as<std::string>(keys[i]));
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
    if (!CharacterVector::is_na(keys[i])) {
      std::string key = as<std::string>(keys[i]);
      filter->add(key);
    }
  }

  return filter;
}

// Function to check keys against the filter
// [[Rcpp::export]]
LogicalVector rcpp_check_keys(XPtr<BloomFilter> filter, CharacterVector keys) {
  LogicalVector result(keys.size());

  for (int i = 0; i < keys.size(); i++) {
    std::string key = as<std::string>(keys[i]);
    result[i] = filter->contains(key);
  }

  return result;
}

// Optimized batch processing function
// [[Rcpp::export]]
LogicalVector rcpp_filter_keys(CharacterVector y_keys, CharacterVector x_keys,
                               size_t expected_elements, double false_positive_rate = 0.01) {
  // Input validation
  if (y_keys.size() == 0) {
    return LogicalVector(x_keys.size(), false);
  }
  
  // Pre-check for NA values in y_keys (do this once)
  bool has_na_in_y = false;
  for (int j = 0; j < y_keys.size() && !has_na_in_y; j++) {
    has_na_in_y = CharacterVector::is_na(y_keys[j]);
  }
  
  // Create the filter from y_keys with optimal sizing
  XPtr<BloomFilter> filter = rcpp_create_filter(y_keys, expected_elements, false_positive_rate);

  // Check all x_keys against the filter - optimized loop
  LogicalVector result(x_keys.size());
  
  for (int i = 0; i < x_keys.size(); i++) {
    if (CharacterVector::is_na(x_keys[i])) {
      result[i] = has_na_in_y;
    } else {
      // Avoid string conversion by using SEXP directly when possible
      std::string key = as<std::string>(x_keys[i]);
      result[i] = filter->contains(key);
    }
  }
  
  return result;
}
