#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <Rcpp.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>

using namespace Rcpp;

// MurmurHash3 implementation (optimized for speed and good distribution)
uint32_t MurmurHash3(const std::string& key, uint32_t seed);

// Second hash function to improve distribution
uint32_t JenkinsHash(const std::string& key, uint32_t seed);

class BloomFilter {
private:
  std::vector<bool> bits;
  size_t m;  // size of bit array
  size_t k;  // number of hash functions
  std::vector<uint32_t> seeds;

public:
  // Constructor
  BloomFilter(size_t expected_elements, double false_positive_rate);
  
  // Add an element to the filter
  void add(const std::string& key);
  
  // Check if an element might be in the filter
  bool contains(const std::string& key) const;
  
  // Get the size of the filter
  size_t size() const;
  
  // Get the number of hash functions
  size_t num_hashes() const;
};

#endif