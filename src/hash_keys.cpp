// File: src/hash_keys.cpp
//
// Composite key -> int32 hasher for bloomjoin (Rcpp).
// Stable across platforms & sessions. Type-aware, NA-aware.
//
// Exports:
//   IntegerVector hash_keys32_cols(List cols,
//                                  bool normalize_strings = true,
//                                  bool na_sentinel = true);
//
// You will call it via an R wrapper that selects the 'by' columns
// and handles edge classes (POSIXlt, integer64 via bit64).

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
  uint64_t h = 1469598103934665603ULL;        // offset basis
  const uint64_t p = 1099511628211ULL;        // FNV prime
  for (size_t i = 0; i < len; ++i) {
    h ^= (uint64_t)data[i];
    h *= p;
  }
  return h;
}

// Canonicalize a double: map -0.0 -> +0.0; map all NA/NaN to a single sentinel.
// Return (sentinel, true) if NA/NaN; otherwise return bit pattern with isNA=false.
static inline std::pair<uint64_t, bool> double_bits_canonical(double d) {
  // R provides macros for NA/NaN checks
  if (ISNAN(d)) { // covers NA and NaN in R
    return std::make_pair(0xD1B54A32D192ED03ULL, true); // NA sentinel
  }
  if (d == 0.0) {
    // canonicalize -0 to +0
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

// Hash one element (row i) of a column to 64-bit; includes type tag mixing.
static inline uint64_t hash_elem(SEXP col, R_xlen_t i, bool normalize_strings) {
  // Factor first (it's INTSXP underneath, but semantics are string-based)
  if (Rf_isFactor(col)) {
    // codes are integers with NA as NA_INTEGER
    int code = INTEGER(col)[i];
    if (code == NA_INTEGER) {
      return mix_in(TAG_FCT, 0xD1B54A32D192ED03ULL);
    }
    SEXP lv = Rf_getAttrib(col, R_LevelsSymbol);
    if (TYPEOF(lv) != STRSXP) {
      // Defensive: should never happen for a valid factor
      return mix_in(TAG_FCT, splitmix64((uint64_t)code));
    }
    // levels are 1-based
    SEXP s = STRING_ELT(lv, code - 1);
    if (s == NA_STRING) {
      return mix_in(TAG_FCT, 0xD1B54A32D192ED03ULL);
    }
    const char* bytes = normalize_strings ? Rf_translateCharUTF8(s) : Rf_translateChar(s);
    uint64_t hv = fnv1a64(reinterpret_cast<const unsigned char*>(bytes), std::strlen(bytes));
    return mix_in(TAG_FCT, splitmix64(hv));
  }

  switch (TYPEOF(col)) {
  case INTSXP: {
    int v = INTEGER(col)[i];
    if (v == NA_INTEGER) {
      return mix_in(TAG_INT, 0xD1B54A32D192ED03ULL);
    } else {
      uint64_t hv = splitmix64((uint64_t)(uint32_t)v);
      return mix_in(TAG_INT, hv);
    }
  }
  case LGLSXP: {
    int v = LOGICAL(col)[i];
    uint64_t code;
    if (v == NA_LOGICAL)      code = 2u;
    else if (v == 0)          code = 0u;
    else                      code = 1u;
    uint64_t hv = splitmix64(code);
    return mix_in(TAG_LGL, hv);
  }
  case REALSXP: {
    // Might be Date or POSIXct; both are REALSXP with class attribute
    bool is_date  = Rf_inherits(col, "Date");
    bool is_posix = Rf_inherits(col, "POSIXct");
    double d = REAL(col)[i];
    auto bits_na = double_bits_canonical(d);
    uint64_t hv = splitmix64(bits_na.first);
    if (is_date)   return mix_in(TAG_DATE, hv);
    if (is_posix)  return mix_in(TAG_POSIX, hv);
    return mix_in(TAG_DBL, hv);
  }
  case STRSXP: {
    SEXP s = STRING_ELT(col, i);
    if (s == NA_STRING) {
      return mix_in(TAG_CHR, 0xD1B54A32D192ED03ULL);
    }
    const char* bytes = normalize_strings ? Rf_translateCharUTF8(s) : Rf_translateChar(s);
    uint64_t hv = fnv1a64(reinterpret_cast<const unsigned char*>(bytes), std::strlen(bytes));
    return mix_in(TAG_CHR, splitmix64(hv));
  }
  default:
    Rcpp::stop("Unsupported column type in hash: SEXP type %d.", TYPEOF(col));
  }
  // not reached
}

// [[Rcpp::export]]
IntegerVector hash_keys32_cols(List cols,
                               bool normalize_strings = true,
                               bool na_sentinel = true) {
  (void)na_sentinel;  // retained for signature compatibility
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

  IntegerVector out(n);
  const uint64_t seed = 0x726F626F746F726FULL; // "robotor", deterministic

  // Accumulate per row
  for (R_xlen_t i = 0; i < n; ++i) {
    uint64_t acc = seed;
    for (R_xlen_t j = 0; j < cols.size(); ++j) {
      SEXP col = cols[j];
      uint64_t hv = hash_elem(col, i, normalize_strings);
      acc = mix_in(acc, hv);
    }
    // final mixdown to 32 bits
    uint64_t final64 = splitmix64(acc);
    uint32_t h32 = (uint32_t)(final64 ^ (final64 >> 32));
    // cast to signed int for R (may be negative; that's OK)
    out[i] = (int32_t)h32;
  }

  return out;
}

