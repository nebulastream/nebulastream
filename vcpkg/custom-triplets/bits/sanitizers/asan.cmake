set(VCPKG_CXX_FLAGS -fsanitize=address)
set(VCPKG_C_FLAGS -fsanitize=address)

# Building LLVM with the `-fsanitize=address` flag causes the sanitizer itself to be built sanitized which is not
# possible. In general if the port supports sanitization via a CMake Option this should be the preferred way, to avoid
# incompatibilities.
if (PORT STREQUAL llvm)
    set(VCPKG_CXX_FLAGS "")
    set(VCPKG_C_FLAGS "")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS -DLLVM_USE_SANITIZER="Address")
endif()
