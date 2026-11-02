# Chainloaded by vcpkg for the main NES project: -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=<this> -DNES_TARGET_ARCH=arm64
include(${CMAKE_CURRENT_LIST_DIR}/arm64-cross-flags.cmake)
find_program(NES_CLANG NAMES clang-22 clang REQUIRED)
find_program(NES_CLANGXX NAMES clang++-22 clang++ REQUIRED)
# Corrosion derives the cargo target triple from these, without them it builds the Rust crates for the host.
set(CMAKE_C_COMPILER_TARGET ${NES_ARM64_TRIPLE})
set(CMAKE_CXX_COMPILER_TARGET ${NES_ARM64_TRIPLE})
set(CMAKE_C_COMPILER ${NES_CLANG})
set(CMAKE_CXX_COMPILER ${NES_CLANGXX})
set(CMAKE_C_FLAGS_INIT "${NES_ARM64_C_COMPILE_FLAGS} -fPIC")
set(CMAKE_ASM_FLAGS_INIT "--target=${NES_ARM64_TRIPLE} --sysroot=${NES_ARM64_SYSROOT}")
# -nostdinc++: do not also pick up the host libc++ headers, they shadow the sysroot ones (float.h include guard clash).
set(CMAKE_CXX_FLAGS_INIT "${NES_ARM64_COMPILE_FLAGS} -nostdinc++ -fPIC")
foreach (_t EXE SHARED MODULE)
    set(CMAKE_${_t}_LINKER_FLAGS_INIT "${NES_ARM64_LINK_FLAGS} -stdlib=libc++ -lc++ -fuse-ld=mold")
endforeach ()
