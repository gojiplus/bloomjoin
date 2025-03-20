#include "BloomFilter.h"

// MurmurHash3 implementation
uint32_t MurmurHash3(const std::string& key, uint32_t seed) {
  // Keep the existing implementation
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

// Jenkins hash implementation
uint32_t JenkinsHash(const std::string& key, uint32_t seed) {
  // Keep the existing implementation
  uint32_t hash = seed;
  for (size_t i = 0; i < key.size(); ++i) {
    hash += key[i];
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  return hash;
}

// BloomFilter constructor implementation
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate) {
  // Calculate optimal filter size and number of hash functions
  m = -(expected_elements * log(false_positive_rate)) / (log(2) * log(2));
  k = round((m / expected_elements) * log(2));

  // Ensure we have at least 1 hash function and a reasonable size
  k = std::max<size_t>(1, std::min<size_t>(k, 10));
  m = std::max<size_t>(expected_elements, m);

  // Initialize the bit array
  bits.resize(m, false);

  // Generate seeds for hash functions
  for (size_t i = 0; i < k; ++i) {
    seeds.push_back(i * 0xf2c35175 + 42);  // Arbitrary but deterministic seeds
  }
}

// Add an element to the filter
void BloomFilter::add(const std::string& key) {
  for (size_t i = 0; i < k; ++i) {
    // Use different hash functions by varying the seed
    uint32_t hash;
    if (i % 2 == 0) {
      hash = MurmurHash3(key, seeds[i]) % m;
    } else {
      hash = JenkinsHash(key, seeds[i]) % m;
    }
    bits[hash] = true;
  }
}

// Check if an element might be in the filter
bool BloomFilter::contains(const std::string& key) const {
  for (size_t i = 0; i < k; ++i) {
    // Use same hash functions as add
    uint32_t hash;
    if (i % 2 == 0) {
      hash = MurmurHash3(key, seeds[i]) % m;
    } else {
      hash = JenkinsHash(key, seeds[i]) % m;
    }
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

// Function to create a filter and add keys
// [[Rcpp::export]]
XPtr<BloomFilter> rcpp_create_filter(CharacterVector keys, size_t expected_elements,
                                     double false_positive_rate = 0.01) {
  // Create filter
  XPtr<BloomFilter> filter(new BloomFilter(expected_elements, false_positive_rate), true);

  // Add all keys
  for (int i = 0; i < keys.size(); i++) {
    std::string key = as<std::string>(keys[i]);
    filter->add(key);
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

// Batch processing function for large sets of keys
// [[Rcpp::export]]
LogicalVector rcpp_filter_keys(CharacterVector y_keys, CharacterVector x_keys,
                               size_t expected_elements, double false_positive_rate = 0.01) {
  // Create the filter from y_keys
  XPtr<BloomFilter> filter = rcpp_create_filter(y_keys, expected_elements, false_positive_rate);

  // Check all x_keys against the filter
  return rcpp_check_keys(filter, x_keys);
}