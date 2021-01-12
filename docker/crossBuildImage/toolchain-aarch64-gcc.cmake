set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(TARGET_ABI "linux-gnu")
SET(CROSS_TARGET ${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_TARGET_ABI})

# specify sysroot for aarch64
SET(CMAKE_SYSROOT /opt/sysroots/aarch64-linux-gnu)

# specify the cross compiler
set(CMAKE_C_COMPILER   aarch64-${TARGET_ABI}-gcc)
set(CMAKE_CXX_COMPILER aarch64-${TARGET_ABI}-g++)

# Target environment containing the required library.
# On Debian-like systems, this is /usr/aarch64-linux-gnu.
set(CMAKE_FIND_ROOT_PATH "/usr/aarch64-${TARGET_ABI}")

# search for programs in the build host directories
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)

# libraries, headers, and packages in the target directories
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

# If these pass, CMake will end up using the host version.
find_program(GCC_FULL_PATH aarch64-${TARGET_ABI}-gcc)
if (NOT GCC_FULL_PATH)
    message(FATAL_ERROR "Cross-compiler aarch64-${TARGET_ABI}-gcc not found")
endif ()