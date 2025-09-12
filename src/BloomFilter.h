#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <Rcpp.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include <unordered_set>

using namespace Rcpp;

// MurmurHash3 implementation (optimized for speed and good distribution)
uint32_t MurmurHash3(const std::string& key, uint32_t seed);

// xxHash32 - faster alternative hash function
uint32_t xxHash32(const std::string& key, uint32_t seed);

// Double hashing using MurmurHash3 with different seeds
uint32_t DoubleHash(const std::string& key, uint32_t seed, uint32_t i, size_t m);

class BloomFilter {
private:
  std::vector<bool> bits;
  size_t m;  // size of bit array
  size_t k;  // number of hash functions
  uint32_t primary_seed;
  uint32_t secondary_seed;
  std::unordered_set<std::string> na_values; // Track NA values separately
  
public:
  // Constructor
  BloomFilter(size_t expected_elements, double false_positive_rate);
  
  // Add an element to the filter
  void add(const std::string& key);
  
  // Check if an element might be in the filter
  bool contains(const std::string& key) const;
  
  // Optimized contains for string views (avoids string copy)
  bool contains_fast(const char* key, size_t length) const;
  
  // Add multiple elements efficiently
  void add_batch(const std::vector<std::string>& keys);
  
  // Reserve capacity for better performance
  void reserve(size_t expected_elements);
  
  // Get the size of the filter
  size_t size() const;
  
  // Get the number of hash functions
  size_t num_hashes() const;
  
  // Get actual number of bits set (for debugging)
  size_t bits_set() const;
  
  // Check if key is NA-like
  static bool is_na_like(const std::string& key);
};

#endif
