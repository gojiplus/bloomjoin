.onLoad <- function(libname, pkgname) {
  if (requireNamespace("waldo", quietly = TRUE)) {
    registerS3method(
      "compare_proxy",
      "bloomjoin",
      compare_proxy.bloomjoin,
      envir = asNamespace("waldo")
    )
  }
}

compare_proxy.bloomjoin <- function(x, path, ...) { # nolint: object_name_linter.
  x <- strip_bloomjoin_attributes(x)
  NextMethod()
}
