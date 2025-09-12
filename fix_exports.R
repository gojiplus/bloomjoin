# Script to fix RcppExports.cpp include issue
# This is needed because Rcpp::compileAttributes() regenerates RcppExports.cpp
# without the necessary #include "BloomFilter.h" header

fix_rcpp_exports <- function() {
  file_path <- "src/RcppExports.cpp"
  if (!file.exists(file_path)) return()
  
  lines <- readLines(file_path)
  
  # Check if BloomFilter.h is already included
  if (any(grepl('#include "BloomFilter.h"', lines))) {
    return()  # Already fixed
  }
  
  # Find the line with #include <Rcpp.h>
  rcpp_line <- which(grepl('#include <Rcpp.h>', lines))
  if (length(rcpp_line) > 0) {
    # Insert the BloomFilter.h include after Rcpp.h
    new_lines <- c(
      lines[1:rcpp_line[1]],
      '#include "BloomFilter.h"',
      lines[(rcpp_line[1]+1):length(lines)]
    )
    writeLines(new_lines, file_path)
    cat("Fixed RcppExports.cpp include\n")
  }
}

# Run the fix automatically
fix_rcpp_exports()