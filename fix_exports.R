# Script to fix RcppExports.cpp include issue
# This is needed because Rcpp::compileAttributes() regenerates RcppExports.cpp
# without the necessary #include "BloomFilter.h" header

fix_rcpp_exports <- function() {
  file_path <- "src/RcppExports.cpp"
  if (!file.exists(file_path)) return()
  
  lines <- readLines(file_path)
  
  # Check if BloomFilter.h is already included
  if (any(grepl('#include "BloomFilter.h"', lines, fixed = TRUE))) {
    cat("RcppExports.cpp already has BloomFilter.h include\n")
    return()  # Already fixed
  }
  
  # Find the line with #include <Rcpp.h>
  rcpp_line <- which(grepl('#include <Rcpp.h>', lines, fixed = TRUE))
  if (length(rcpp_line) > 0) {
    # Insert the BloomFilter.h include after Rcpp.h
    new_lines <- c(
      lines[1:rcpp_line[1]],
      '#include "BloomFilter.h"',
      lines[(rcpp_line[1]+1):length(lines)]
    )
    writeLines(new_lines, file_path)
    cat("Fixed RcppExports.cpp include\n")
  } else {
    cat("Could not find #include <Rcpp.h> line\n")
  }
}

# Load package with automatic fix
load_with_fix <- function() {
  # First run compileAttributes which may regenerate RcppExports.cpp
  cat("Running Rcpp::compileAttributes()...\n")
  Rcpp::compileAttributes()
  
  # Then fix the exports
  fix_rcpp_exports()
  
  # Finally load the package
  cat("Loading package...\n")
  devtools::load_all()
}

# Run the fix automatically
fix_rcpp_exports()