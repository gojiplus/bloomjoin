#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <Rcpp.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include <cstdint>

using namespace Rcpp;

// BitArray: efficient bit storage using 64-bit words
// Replaces std::vector<bool> for better cache behavior
class BitArray {
private:
  std::vector<uint64_t> words;
  size_t num_bits;

public:
  BitArray() : num_bits(0) {}

  explicit BitArray(size_t n) : num_bits(n) {
    size_t num_words = (n + 63) >> 6;
    words.resize(num_words, 0ULL);
  }

  void resize(size_t n) {
    num_bits = n;
    size_t num_words = (n + 63) >> 6;
    words.assign(num_words, 0ULL);
  }

  inline void set(size_t i) {
    words[i >> 6] |= (1ULL << (i & 63));
  }

  inline bool test(size_t i) const {
    return (words[i >> 6] & (1ULL << (i & 63))) != 0;
  }

  size_t count() const {
    size_t c = 0;
    for (size_t i = 0; i < words.size(); ++i) {
      c += __builtin_popcountll(words[i]);
    }
    return c;
  }

  size_t size() const { return num_bits; }
};

// Fast integer hash using splitmix64 finalizer
inline uint32_t hash_int32(int32_t key, uint32_t seed) {
  uint64_t x = static_cast<uint64_t>(static_cast<uint32_t>(key)) ^ static_cast<uint64_t>(seed);
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  x = x ^ (x >> 31);
  return static_cast<uint32_t>(x);
}

// Second hash for double hashing (different constant)
inline uint32_t hash_int32_secondary(int32_t key, uint32_t seed) {
  uint64_t x = static_cast<uint64_t>(static_cast<uint32_t>(key)) ^ static_cast<uint64_t>(seed);
  x += 0x85ebca6b27d4eb4fULL;
  x = (x ^ (x >> 30)) * 0x94d049bb133111ebULL;
  x = (x ^ (x >> 27)) * 0xbf58476d1ce4e5b9ULL;
  x = x ^ (x >> 31);
  return static_cast<uint32_t>(x) | 1;  // Ensure odd for double hashing
}

// Sizing for a target false positive rate, shared by the filter itself and by
// the bloom_params() helper so the helper cannot describe a filter the package
// does not build.
//
// Standard Bloom sizing (Broder & Mitzenmacher): p needs -ln(p)/ln(2)^2 bits
// per key, and m bits holding n keys are best probed (m/n)*ln(2) times. This
// is what Guava's optimalNumOfBits/optimalNumOfHashFunctions and Spark's
// BloomFilter.create compute.
inline void bloom_sizing(size_t expected_elements, double false_positive_rate,
                         size_t* m_out, size_t* k_out) {
  if (expected_elements == 0) expected_elements = 1;
  if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0) {
    false_positive_rate = 0.01;
  }

  const double ln2 = 0.6931471805599453;
  const double m_raw =
      (-std::log(false_positive_rate) / (ln2 * ln2)) * static_cast<double>(expected_elements);

  // Round up to a power of two so the modulo stays a mask. Cap at 2^40 bits so
  // an extreme rate cannot overflow the shift.
  const size_t max_bits = static_cast<size_t>(1) << 40;
  size_t power = 1;
  while (static_cast<double>(power) < m_raw && power < max_bits) power <<= 1;

  // k from the rounded m, not from m_raw: rounding up adds as much as 2x the
  // bits, and spending them on probes is what keeps the achieved rate at or
  // under the requested one.
  const double k_opt =
      (static_cast<double>(power) / static_cast<double>(expected_elements)) * ln2;
  size_t k = static_cast<size_t>(k_opt + 0.5);
  if (k < 1) k = 1;
  if (k > 30) k = 30;

  *m_out = power;
  *k_out = k;
}

class BloomFilter {
private:
  BitArray bits;
  size_t m;  // size of bit array (power of 2)
  size_t k;  // number of hash functions
  uint32_t primary_seed;
  uint32_t secondary_seed;
  bool has_na;  // Track if NA was added

public:
  BloomFilter(size_t expected_elements, double false_positive_rate);

  // Integer-native methods (no string conversion)
  void add_int(int32_t key);
  bool contains_int(int32_t key) const;

  // Batch operations for integers
  void add_int_batch(const int* keys, size_t n);

  // NA handling
  void add_na() { has_na = true; }
  bool contains_na() const { return has_na; }

  // Stats
  size_t size() const { return m; }
  size_t num_hashes() const { return k; }
  size_t bits_set() const { return bits.count(); }
};

#endif
