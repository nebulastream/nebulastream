# Cross compile flags for x64 -> arm64 linux with clang + libc++. Shared by the main project toolchain and the vcpkg
# toolchain used to build the dependencies. Requires NES_ARM64_SYSROOT (cmake var or env) to point to an arm64 sysroot
# (debian arm64 packages: libc6-dev, libgcc-*-dev, libc++-dev, libc++abi-dev, libunwind-dev, linux-libc-dev, ...).
if (NOT NES_ARM64_SYSROOT)
    set(NES_ARM64_SYSROOT "$ENV{NES_ARM64_SYSROOT}")
endif ()
if (NOT NES_ARM64_SYSROOT)
    message(FATAL_ERROR "Cross compiling for arm64 requires NES_ARM64_SYSROOT")
endif ()

set(CMAKE_SYSTEM_NAME Linux CACHE STRING "")
set(CMAKE_SYSTEM_PROCESSOR aarch64 CACHE STRING "")
set(CMAKE_CROSSCOMPILING ON CACHE BOOL "")
set(CMAKE_SYSROOT "${NES_ARM64_SYSROOT}")
set(NES_ARM64_TRIPLE aarch64-linux-gnu)

file(GLOB _NES_GCC_DIRS "${NES_ARM64_SYSROOT}/usr/lib/gcc/${NES_ARM64_TRIPLE}/*")
list(GET _NES_GCC_DIRS -1 NES_ARM64_GCC_DIR)

# libc++'s wrapper headers (limits.h, ...) break glibc feature macros in C compilations, so C does not get its include path.
set(NES_ARM64_C_COMPILE_FLAGS "--target=${NES_ARM64_TRIPLE} --sysroot=${NES_ARM64_SYSROOT} -isystem ${NES_ARM64_SYSROOT}/usr/include/${NES_ARM64_TRIPLE} -isystem ${NES_ARM64_SYSROOT}/usr/include")
set(NES_ARM64_COMPILE_FLAGS "--target=${NES_ARM64_TRIPLE} --sysroot=${NES_ARM64_SYSROOT} -isystem ${NES_ARM64_SYSROOT}/usr/lib/llvm-19/include/c++/v1 -isystem ${NES_ARM64_SYSROOT}/usr/include/${NES_ARM64_TRIPLE} -isystem ${NES_ARM64_SYSROOT}/usr/include")
set(NES_ARM64_LINK_FLAGS "--target=${NES_ARM64_TRIPLE} --sysroot=${NES_ARM64_SYSROOT} -B${NES_ARM64_GCC_DIR} -L${NES_ARM64_GCC_DIR} -L${NES_ARM64_SYSROOT}/usr/lib/${NES_ARM64_TRIPLE} -L${NES_ARM64_SYSROOT}/usr/lib/llvm-19/lib -Wl,-rpath-link,${NES_ARM64_SYSROOT}/usr/lib/${NES_ARM64_TRIPLE} -Wl,-rpath-link,${NES_ARM64_SYSROOT}/usr/lib/llvm-19/lib")

# Never pick up host libraries/headers, but do run host programs (protoc, ...).
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
