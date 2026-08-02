## A Bloom filter may produce false positives but never false negatives, so
## bloom_join() must return exactly what the corresponding dplyr join returns.
##
## The hash used to mix a per-storage-type tag into every key and to hash
## integers from their int32 bits rather than their numeric value. dplyr matches
## 1L to 1.0 and a factor level to the equal character string, so those keys hash
## differently on the two sides, the probe missed, and genuine matches were
## dropped -- 99% of them in the case below. The existing suite could not catch
## it because every type test used the same type on both frames.

cross_type_frames <- function(kx, ky, n = 3000) {
  list(x = data.frame(id = kx, vx = seq_len(n)),
       y = data.frame(id = ky, vy = seq_len(n) * 10))
}

test_that("keys that dplyr considers equal are joined regardless of storage type", {
  # Must exceed the 1024-row probe threshold in should_skip_prefilter(),
  # otherwise the Bloom prefilter is bypassed and the bug cannot appear.
  n <- 3000
  cases <- list(
    "integer vs double"   = list(seq_len(n), as.numeric(seq_len(n))),
    "double vs integer"   = list(as.numeric(seq_len(n)), seq_len(n)),
    "factor vs character" = list(factor(paste0("k", seq_len(n))), paste0("k", seq_len(n))),
    "character vs factor" = list(paste0("k", seq_len(n)), factor(paste0("k", seq_len(n)))),
    "Date double vs int"  = list(as.Date(seq_len(n), origin = "1970-01-01"),
                                 structure(as.integer(seq_len(n)), class = "Date"))
  )

  for (nm in names(cases)) {
    fr <- cross_type_frames(cases[[nm]][[1]], cases[[nm]][[2]], n)
    for (ty in c("inner", "left", "semi", "anti")) {
      got <- suppressMessages(bloom_join(fr$x, fr$y, by = "id", type = ty))
      ref <- suppressMessages(
        switch(ty,
               inner = dplyr::inner_join(fr$x, fr$y, by = "id"),
               left  = dplyr::left_join(fr$x, fr$y, by = "id"),
               semi  = dplyr::semi_join(fr$x, fr$y, by = "id"),
               anti  = dplyr::anti_join(fr$x, fr$y, by = "id")))
      expect_equal(as.data.frame(got), as.data.frame(ref),
                   ignore_attr = TRUE,
                   info = paste(nm, ty))
    }
  }
})

test_that("a left join across key types does not silently NA the payload", {
  # The nastiest form: the row count matched dplyr exactly while the payload
  # was NA-ed out, so any row-count sanity check passed.
  n <- 3000
  fr <- cross_type_frames(seq_len(n), as.numeric(seq_len(n)), n)
  got <- suppressMessages(bloom_join(fr$x, fr$y, by = "id", type = "left"))
  expect_equal(nrow(got), n)
  expect_equal(sum(is.na(got$vy)), 0)
})

test_that("distinct values still hash distinctly within a type", {
  # Unifying the type tags must not collapse genuinely different keys.
  n <- 3000
  x <- data.frame(id = seq_len(n), vx = seq_len(n))
  y <- data.frame(id = seq_len(n) + n, vy = seq_len(n))
  expect_equal(nrow(suppressMessages(bloom_join(x, y, by = "id"))), 0)
})

test_that("logical and datetime keys match the numerics dplyr matches them to", {
  # Tested as the second half of a composite key, so the unique id keeps the
  # join one-to-one; a constant logical key on its own is many-to-many.
  #
  # dplyr promotes logical to integer to double, so TRUE must hash as 1 does.
  # A Date counts days while a POSIXct counts seconds, and dplyr promotes Date
  # to datetime, so equal instants must hash alike.
  n <- 3000
  dates <- as.Date(seq_len(n), origin = "1970-01-01")
  cases <- list(
    "logical vs integer" = list(rep(TRUE, n), rep(1L, n)),
    "logical vs double"  = list(rep(TRUE, n), rep(1, n)),
    "FALSE vs zero"      = list(rep(FALSE, n), rep(0L, n)),
    "Date vs POSIXct"    = list(dates, as.POSIXct(dates, tz = "UTC"))
  )

  for (nm in names(cases)) {
    x <- data.frame(id = seq_len(n), k = cases[[nm]][[1]], vx = seq_len(n))
    y <- data.frame(id = seq_len(n), k = cases[[nm]][[2]], vy = seq_len(n))
    got <- suppressMessages(bloom_join(x, y, by = c("id", "k"), type = "inner"))
    ref <- suppressMessages(dplyr::inner_join(x, y, by = c("id", "k")))
    expect_equal(nrow(got), nrow(ref), info = nm)
    expect_equal(nrow(got), n, info = nm)
  }
})
