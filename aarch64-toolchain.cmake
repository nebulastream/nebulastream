# aarch64-toolchain.cmake
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

# Specify the cross-compiler
set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)

# Specify the target environment (sysroot)
set(CMAKE_FIND_ROOT_PATH /data/ntantow/ESPAT/zcu106_sysroot)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

# Include the vcpkg toolchain file
include(/data/ntantow/ESPAT/vcpkg/scripts/buildsystems/vcpkg.cmake)

string(REPLACE "-Wconstant-conversion" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-Wbitwise-instead-of-logical" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
