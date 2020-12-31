set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(TARGET_ABI "linux-gnu")

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

# If we don't set these, CMake will end up using the host version.
find_program(GCC_FULL_PATH aarch64-${TARGET_ABI}-gcc)
if (NOT GCC_FULL_PATH)
    message(FATAL_ERROR "Cross-compiler aarch64-${TARGET_ABI}-gcc not found")
endif ()
get_filename_component(GCC_DIR ${GCC_FULL_PATH} PATH)
set(CMAKE_LINKER       ${GCC_DIR}/aarch64-${TARGET_ABI}-ld      CACHE FILEPATH "linker")
set(CMAKE_ASM_COMPILER ${GCC_DIR}/aarch64-${TARGET_ABI}-as      CACHE FILEPATH "assembler")
set(CMAKE_OBJCOPY      ${GCC_DIR}/aarch64-${TARGET_ABI}-objcopy CACHE FILEPATH "objcopy")
set(CMAKE_STRIP        ${GCC_DIR}/aarch64-${TARGET_ABI}-strip   CACHE FILEPATH "strip")
set(CMAKE_CPP          ${GCC_DIR}/aarch64-${TARGET_ABI}-cpp     CACHE FILEPATH "cpp")