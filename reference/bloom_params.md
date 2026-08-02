# Report the Bloom filter chosen for a target false positive rate

Given an expected number of distinct keys `n` and a target false
positive rate `p`, report the filter [`bloom_join()`](bloom_join.md)
will build: total bits `m`, number of hash functions `k`, and the false
positive rate that combination achieves.

## Usage

``` r
bloom_params(n, p = 0.01)
```

## Arguments

- n:

  Numeric scalar (\> 0): expected number of distinct keys to insert.
  Fractional values are allowed (estimates).

- p:

  Numeric scalar in (0, 1): target false positive rate.

## Value

A list with class "bloom_params" containing:

- n: input n

- p_target: target p

- m_bits: total bits, a power of two

- bytes: total bytes

- bits_per_key: m_bits / n

- k: number of hash functions

- fpr_est: the rate that (m, n, k) achieves

## Details

The numbers come from the same C++ routine the filter itself uses, so
this cannot describe a filter the package does not build. It used to: it
advertised a blocked layout, with a `block_bits` argument and a `blocks`
count, that the filter has never implemented, and reported an `m` and
`k` the filter did not use.

Sizing is the standard one (Broder & Mitzenmacher), as used by Guava's
`BloomFilter.create` and Spark's: bits_per_key = -log(p) / (log(2)^2) k
= log(2) \* (m / n) fpr(m, n, k) = (1 - exp(-k \* n / m))^k

`m` is then rounded up to a power of two, because the filter indexes
with a bit mask rather than a modulo, and `k` is taken from the rounded
`m`. Rounding only ever adds bits, so the achieved rate lands at or
under the request.

## Examples

``` r
bp <- bloom_params(1e6, 1e-2)
bp
#> Bloom filter for a target false positive rate
#>   n (expected keys): 1,000,000
#>   target FPR:        0.01
#>   total bits (m):    16,777,216
#>   total bytes:       2,097,152 (2 MiB)
#>   bits per key:      16.78
#>   hashes (k):        12
#>   achieved FPR:      0.0003165
```
