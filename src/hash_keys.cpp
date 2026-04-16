// File: src/hash_keys.cpp
//
// Composite key -> int32 hasher for bloomjoin (Rcpp).
// Optimized with column-major processing for better cache locality.
// Stable across platforms & sessions. Type-aware, NA-aware.
//
// Exports:
//   IntegerVector hash_keys32_cols(List cols,
//                                  bool normalize_strings = true,
//                                  bool na_sentinel = true);

#include <Rcpp.h>
#include <cstdint>
#include <cstring>
#include <vector>

using namespace Rcpp;

// ---------- Utilities ----------

static inline uint64_t rotl64(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

static inline uint64_t splitmix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  x = x ^ (x >> 31);
  return x;
}

// FNV-1a 64-bit over byte buffer (stable, simple)
static inline uint64_t fnv1a64(const unsigned char* data, size_t len) {
  uint64_t h = 1469598103934665603ULL;
  const uint64_t p = 1099511628211ULL;
  for (size_t i = 0; i < len; ++i) {
    h ^= (uint64_t)data[i];
    h *= p;
  }
  return h;
}

// Canonicalize a double: map -0.0 -> +0.0; map all NA/NaN to a single sentinel.
static inline std::pair<uint64_t, bool> double_bits_canonical(double d) {
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

// Type tags (mixed in to keep types distinct even when payloads collide)
static constexpr uint64_t TAG_INT    = 0x9ae16a3b2f90404fULL;
static constexpr uint64_t TAG_DBL    = 0xc949d7c7509e6557ULL;
static constexpr uint64_t TAG_CHR    = 0x8a5cd789635d2dffULL;
static constexpr uint64_t TAG_LGL    = 0x85ebca6b27d4eb4fULL;
static constexpr uint64_t TAG_FCT    = 0x4cf5ad432745937fULL;
static constexpr uint64_t TAG_DATE   = 0x94d049bb133111ebULL;
static constexpr uint64_t TAG_POSIX  = 0xbf58476d1ce4e5b9ULL;

// Combine one component hash into an accumulator.
static inline uint64_t mix_in(uint64_t acc, uint64_t h) {
  acc ^= rotl64(h, 23);
  acc *= 0x9e3779b97f4a7c15ULL;
  return acc;
}

// [[Rcpp::export]]
IntegerVector hash_keys32_cols(List cols,
                               bool normalize_strings = true,
                               bool na_sentinel = true) {
  (void)na_sentinel;
  if (cols.size() == 0) {
    stop("`cols` must contain at least one column.");
  }

  // Determine number of rows and check consistent lengths
  R_xlen_t n = -1;
  for (R_xlen_t j = 0; j < cols.size(); ++j) {
    SEXP col = cols[j];
    if (!Rf_isVector(col))
      stop("All elements of `cols` must be atomic vectors or factors.");
    if (n < 0) n = Rf_xlength(col);
    if (Rf_xlength(col) != n)
      stop("All columns must have the same length.");
  }

  // Initialize hash accumulators
  const uint64_t seed = 0x726F626F746F726FULL;
  std::vector<uint64_t> hashes(n, seed);

  // Column-major processing: process each column fully before moving to next
  // This is cache-friendly for R's column-major storage
  for (R_xlen_t col_idx = 0; col_idx < cols.size(); ++col_idx) {
    SEXP col = cols[col_idx];

    // Factor handling
    if (Rf_isFactor(col)) {
      SEXP lv = Rf_getAttrib(col, R_LevelsSymbol);
      const int* codes = INTEGER(col);

      // Pre-hash all levels for efficient lookup
      R_xlen_t n_levels = Rf_length(lv);
      std::vector<uint64_t> level_hashes(n_levels);
      for (R_xlen_t li = 0; li < n_levels; ++li) {
        SEXP s = STRING_ELT(lv, li);
        if (s == NA_STRING) {
          level_hashes[li] = mix_in(TAG_FCT, 0xD1B54A32D192ED03ULL);
        } else {
          const char* bytes = normalize_strings ? Rf_translateCharUTF8(s) : Rf_translateChar(s);
          uint64_t hv = fnv1a64(reinterpret_cast<const unsigned char*>(bytes), std::strlen(bytes));
          level_hashes[li] = mix_in(TAG_FCT, splitmix64(hv));
        }
      }

      // Process all rows for this column
      for (R_xlen_t i = 0; i < n; ++i) {
        int code = codes[i];
        uint64_t hv;
        if (code == NA_INTEGER) {
          hv = mix_in(TAG_FCT, 0xD1B54A32D192ED03ULL);
        } else {
          hv = level_hashes[code - 1];
        }
        hashes[i] = mix_in(hashes[i], hv);
      }
      continue;
    }

    switch (TYPEOF(col)) {
    case INTSXP: {
      const int* data = INTEGER(col);
      R_xlen_t i = 0;

      // Process 4 at a time with prefetch
      for (; i + 4 <= n; i += 4) {
        if (i + 16 < n) {
          __builtin_prefetch(&data[i + 16], 0, 0);
        }
        for (R_xlen_t j = 0; j < 4; ++j) {
          int v = data[i + j];
          uint64_t hv;
          if (v == NA_INTEGER) {
            hv = mix_in(TAG_INT, 0xD1B54A32D192ED03ULL);
          } else {
            hv = mix_in(TAG_INT, splitmix64(static_cast<uint64_t>(static_cast<uint32_t>(v))));
          }
          hashes[i + j] = mix_in(hashes[i + j], hv);
        }
      }
      // Handle remainder
      for (; i < n; ++i) {
        int v = data[i];
        uint64_t hv;
        if (v == NA_INTEGER) {
          hv = mix_in(TAG_INT, 0xD1B54A32D192ED03ULL);
        } else {
          hv = mix_in(TAG_INT, splitmix64(static_cast<uint64_t>(static_cast<uint32_t>(v))));
        }
        hashes[i] = mix_in(hashes[i], hv);
      }
      break;
    }
    case LGLSXP: {
      const int* data = LOGICAL(col);
      for (R_xlen_t i = 0; i < n; ++i) {
        int v = data[i];
        uint64_t code;
        if (v == NA_LOGICAL) code = 2u;
        else if (v == 0) code = 0u;
        else code = 1u;
        uint64_t hv = mix_in(TAG_LGL, splitmix64(code));
        hashes[i] = mix_in(hashes[i], hv);
      }
      break;
    }
    case REALSXP: {
      const double* data = REAL(col);
      bool is_date = Rf_inherits(col, "Date");
      bool is_posix = Rf_inherits(col, "POSIXct");
      uint64_t tag = is_date ? TAG_DATE : (is_posix ? TAG_POSIX : TAG_DBL);

      R_xlen_t i = 0;
      // Process 4 at a time with prefetch
      for (; i + 4 <= n; i += 4) {
        if (i + 16 < n) {
          __builtin_prefetch(&data[i + 16], 0, 0);
        }
        for (R_xlen_t j = 0; j < 4; ++j) {
          auto bits_na = double_bits_canonical(data[i + j]);
          uint64_t hv = mix_in(tag, splitmix64(bits_na.first));
          hashes[i + j] = mix_in(hashes[i + j], hv);
        }
      }
      // Handle remainder
      for (; i < n; ++i) {
        auto bits_na = double_bits_canonical(data[i]);
        uint64_t hv = mix_in(tag, splitmix64(bits_na.first));
        hashes[i] = mix_in(hashes[i], hv);
      }
      break;
    }
    case STRSXP: {
      for (R_xlen_t i = 0; i < n; ++i) {
        SEXP s = STRING_ELT(col, i);
        uint64_t hv;
        if (s == NA_STRING) {
          hv = mix_in(TAG_CHR, 0xD1B54A32D192ED03ULL);
        } else {
          const char* bytes = normalize_strings ? Rf_translateCharUTF8(s) : Rf_translateChar(s);
          uint64_t fhv = fnv1a64(reinterpret_cast<const unsigned char*>(bytes), std::strlen(bytes));
          hv = mix_in(TAG_CHR, splitmix64(fhv));
        }
        hashes[i] = mix_in(hashes[i], hv);
      }
      break;
    }
    default:
      Rcpp::stop("Unsupported column type in hash: SEXP type %d.", TYPEOF(col));
    }
  }

  // Final mixdown to 32 bits
  IntegerVector out(n);
  for (R_xlen_t i = 0; i < n; ++i) {
    uint64_t final64 = splitmix64(hashes[i]);
    uint32_t h32 = static_cast<uint32_t>(final64 ^ (final64 >> 32));
    out[i] = static_cast<int32_t>(h32);
  }

  return out;
}
