## bloomjoin

[![CI](https://github.com/gojiplus/bloomjoin/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gojiplus/bloomjoin/actions/workflows/R-CMD-check.yaml)
[![CRAN\_Status\_Badge](http://www.r-pkg.org/badges/version/bloomjoin)](https://cran.r-project.org/package=bloomjoin)
![](http://cranlogs.r-pkg.org/badges/grand-total/bloomjoin)

Faster, memory-efficient joins when joining a large table to a small lookup table.

### When to use

bloomjoin helps when:
- **Large table joined to small table** (10:1 ratio or more)
- **Low overlap** between join keys (<25%)

| n_x | n_y | overlap | speed | memory |
|----:|----:|--------:|------:|-------:|
| 1,000,000 | 10,000 | 1% | 2.0x | 2.2x |
| 1,000,000 | 10,000 | 5% | 1.6x | 2.0x |
| 500,000 | 5,000 | 2% | 1.7x | 1.9x |
| 200,000 | 20,000 | 5% | 1.2x | 1.2x |

Values > 1 mean bloomjoin is faster / uses less memory than dplyr.

### Installation

```r
devtools::install_github("gojiplus/bloomjoin")
```

### Usage

```r
library(bloomjoin)

result <- bloom_join(large_df, small_lookup, by = "id")
```

Same syntax as dplyr. Supports `type = "inner"`, `"left"`, `"right"`, `"semi"`, `"anti"`.

### When NOT to use

- **Similar-sized tables**: dplyr is faster
- **High overlap** (>50%): no benefit from pre-filtering

| n_x | n_y | overlap | speed | memory |
|----:|----:|--------:|------:|-------:|
| 100,000 | 100,000 | 10% | 0.4x | 0.5x |
| 100,000 | 100,000 | 50% | 0.4x | 0.4x |

Values < 1 mean dplyr is faster.

### How it works

1. Build a Bloom filter from the smaller table's keys
2. Pre-filter the larger table to remove non-matching rows
3. Run the actual join on the reduced dataset

Bloom filters have no false negatives, so no matches are lost.

### Documentation

- [Getting Started](https://gojiplus.github.io/bloomjoin/articles/bloomjoin-guide.html)
- [Performance Analysis](https://gojiplus.github.io/bloomjoin/articles/benchmarking-bloomjoin.html)
- [Function Reference](https://gojiplus.github.io/bloomjoin/reference/)

### License

MIT
