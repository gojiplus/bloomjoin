# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
- `devtools::load_all()` - Load package for interactive development
- `devtools::test()` or `testthat::test_check("bloomjoin")` - Run all tests
- Test files are in `tests/testthat/` with the main test file being `test-bloom_join.R`

### Building and Checking
- `devtools::build()` - Build the package
- `devtools::check()` - Run R CMD check for comprehensive package validation
- `devtools::install()` - Install the package locally

### Documentation
- `devtools::document()` - Generate documentation from roxygen comments
- `pkgdown::build_site()` - Build the package website (outputs to `docs/`)

## Architecture Overview

This is an R package implementing Bloom filter-based joins for improved performance with large datasets. The package combines R and C++ code using Rcpp for optimal performance.

### Core Components

**R Layer (`R/bloomjoin.R`)**:
- `bloom_join()` - Main function implementing the Bloom filter join algorithm
- `perform_standard_join()` - Internal wrapper around dplyr join functions
- Handles multiple join types: inner, left, right, full, semi, anti
- Manages composite keys for multi-column joins
- Validates inputs and handles edge cases

**C++ Layer (`src/`)**:
- `BloomFilter.h/.cpp` - Core Bloom filter implementation with MurmurHash3 and JenkinsHash
- `RcppExports.cpp` - Rcpp-generated bindings
- Key functions: `rcpp_create_filter()`, `rcpp_check_keys()`, `rcpp_filter_keys()`

### Algorithm Flow

1. Create hash set from lookup table (y) keys using Bloom filter
2. Filter primary table (x) to only include rows with keys that might exist in y
3. Perform standard dplyr join on the pre-filtered dataset

### Key Technical Details

- Uses two hash functions (MurmurHash3 and JenkinsHash) for optimal distribution
- Automatically calculates optimal Bloom filter size based on input data
- Supports named join vectors for different column names between tables
- Creates temporary composite keys for multi-column joins
- Falls back to standard joins for certain join types (left, full) where pre-filtering isn't beneficial

### Dependencies

- **Required**: dplyr (for actual join operations), Rcpp (C++ integration)
- **Testing**: testthat, tibble
- Package follows standard R package structure with DESCRIPTION, NAMESPACE, man/ docs

### Testing Strategy

Tests validate that Bloom joins produce identical results to standard dplyr joins across various scenarios:
- Different join types and data sizes
- Multi-column joins and composite keys  
- Edge cases (no overlap, complete overlap, duplicated keys, NA values)
- Custom Bloom filter parameters