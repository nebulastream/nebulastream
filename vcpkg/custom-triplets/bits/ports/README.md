# Per-port triplet overrides

After architecture and sanitizer defaults, the loader applies existing files
in this order (later files override earlier settings):

1. `<port>.cmake`
2. `<port>-<architecture>.cmake`
3. `<port>-<sanitizer>.cmake`
4. `<port>-<architecture>-<sanitizer>.cmake`

Architectures are `x64` and `arm64`; sanitizers are `asan`, `tsan`, and
`ubsan`. Unsanitized triplets skip the sanitizer-specific layers.
All files are optional.

For example, `openblas-x64-asan.cmake` applies only when building OpenBLAS
for x64 with AddressSanitizer.

The loader automatically adds applicable override files to
`VCPKG_HASH_ADDITIONAL_FILES`; override files do not need to register themselves.
Editing an override invalidates that port's cache and its dependents, not
unrelated ports. Editing the shared loader still affects all ports.
