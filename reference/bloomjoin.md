# BloomJoin: Efficient data.frame joins using probabilistic prefilters

[`bloom_join()`](bloom_join.md) wraps the standard dplyr join verbs with
a probabilistic pre-filter stage implemented in C++ via Rcpp. The filter
trims the rows that reach the expensive join when the overlap between
the two tables is small, yielding faster joins without changing the
final result.

## Details

The exported surface mirrors the dplyr join verbs while adding controls
for the pre-filter:

- `engine` selects the probabilistic data structure (currently only
  Bloom filters are implemented).

- `prefilter_side` decides which input will be filtered before calling
  the underlying join.

- `fpr` configures the target false positive rate for the Bloom filter.

- `n_hint` allows callers to pass optional distinct-count hints for the
  join keys which helps size the filter without extra scans.

The function automatically samples the key columns to estimate distinct
counts, chooses which side to pre-filter when `prefilter_side = "auto"`,
and falls back to the raw dplyr join when the Bloom filter would not
remove enough rows to pay for its construction.

## See also

Useful links:

- <https://gojiplus.github.io/bloomjoin/>

- <https://github.com/gojiplus/bloomjoin>

- Report bugs at <https://github.com/gojiplus/bloomjoin/issues>

## Author

**Maintainer**: Gaurav Sood <gsood07@gmail.com>

Authors:

- Gaurav Sood <gsood07@gmail.com>
