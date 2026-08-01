# Choose Bloom filter parameters for a blocked layout

Given an expected number of distinct keys `n` and target false positive
rate `p`, compute total bits `m` (rounded up to a multiple of
`block_bits`), number of hash functions `k`, the achieved false positive
rate after rounding, and related quantities.

## Usage

``` r
bloom_params(
  n,
  p = 1e-2,
  block_bits = 512L,
  min_k = 1L,
  max_k = 12L,
  round_to = block_bits
)

# S3 method for class 'bloom_params'
print(x, ...)
```

## Arguments

- n:

  Numeric scalar (\> 0): expected number of distinct keys to insert.
  Fractional values are allowed (estimates).

- p:

  Numeric scalar in (0, 1): target false positive rate.

- block_bits:

  Integer (default 512): block size in bits for a blocked Bloom filter.
  512 corresponds to one 64-byte cache line.

- min_k, max_k:

  Integers giving the bounds for the number of hash functions (defaults
  1 and 12).

- round_to:

  Integer or `NULL`. Round `m` up to a multiple of this many bits.
  Defaults to `block_bits`; use `NULL` to disable rounding.

- x:

  A `bloom_params` object.

- ...:

  Additional arguments passed to
  [`print()`](https://rdrr.io/r/base/print.html). Currently ignored.

## Value

A list with class `"bloom_params"` containing the inputs and derived
parameters:

- `n`: the input `n`.

- `p_target`: the target false positive rate.

- `block_bits`: the block size used when rounding.

- `m_bits`: total number of bits after rounding.

- `bytes`: total number of bytes.

- `bits_per_key`: ratio of `m_bits` to `n`.

- `k`: number of hash functions selected.

- `fpr_est`: estimated false positive rate after rounding.

- `blocks`: number of blocks (`m_bits / block_bits`).

## Details

The helper uses the standard Bloom filter relationships:
\$\$\mathrm{bits\\per\\key} = -\log(p) / (\log(2)^2)\$\$
\$\$k\_\mathrm{opt} = \log(2) \cdot (m / n)\$\$ \$\$\mathrm{fpr}(m, n,
k) = (1 - \exp(-k n / m))^k\$\$ The `m` value is rounded to a multiple
of `round_to` (defaulting to `block_bits`) so that the resulting Bloom
filter fits evenly into cache-line-sized blocks.

## Examples

``` r
bp <- bloom_params(1e6, 1e-2)
bp
#> Bloom parameters (blocked)
#>   n (expected keys): 1,000,000
#>   target FPR:        0.01
#>   block bits:        512
#>   total bits (m):    9,585,152
#>   total bytes:       1,198,144 (1.14 MiB)
#>   bits per key:      9.585
#>   hashes (k):        7
#>   achieved FPR:      0.01004
#>   blocks:            18721
# Expect k ~ 7 and about 1.14 MiB of memory with 512-bit blocks.
```
