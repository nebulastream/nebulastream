
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Detects race conditions.
# -fno-omit-frame-pointer Instruct the compiler to store the stack frame pointer inside a register to get better stack
#                         traces.
# Flags: https://github.com/google/sanitizers/wiki/ThreadSanitizerFlags
#
# Slowdown and memory overhead of this cmake profile is reported to be:
# Slowdown:         5x - 15x.
# Memory overhead:  5x - 10x.
#
# Attention: 1. ThreadSanitizer maps (but does not reserve) a lot of virtual address space.
#               This means that tools like ulimit may not work as usually expected.
#            2. Libc/libstdc++ static linking is not supported.
#            3. Non-position-independent executables are not supported (clang++ acts as though -fPIE and -fPIC had
#                                                                       been supplied).
#
set(CMAKE_CXX_FLAGS_THREAD "-fsanitize=thread -fno-omit-frame-pointer -fno-optimize-sibling-calls -Werror=return-type -DTSAN_OPTIONS=second_deadlock_stack=1 -g -Og ${NES_SPECIFIC_FLAGS}")
set(CMAKE_LINKER_FLAGS_THREAD "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
# Continue after first error. Attention: Errors after the first error might be spurious. Use with caution.
set(CMAKE_CXX_FLAGS_THREAD_RECOVER "${CMAKE_CXX_FLAGS_THREAD} -fsanitize-recover=all")
set(CMAKE_LINKER_FLAGS_THREAD_RECOVER "${CMAKE_LINKER_FLAGS_THREAD}")


# Detects uninitialized reads and conditional jumps based on uninitialized memory.
# -fsanitize-memory-track-origins=2 track the origins of uninitialized value (like Valgrind's track origins option)
# -fno-optimize-sibling-calls       do not optimize recursion (better stack traces).
# -fsanitize-memory-use-after-dtor  has experimental support and can be added if desired.
# Flags: https://github.com/google/sanitizers/wiki/SanitizerCommonFlags
#
# Slowdown and memory overhead of this cmake profile is reported to be:
# Slowdown:         3x - 6x (with origin tracking enabled).
# Memory overhead:  2x - 3x
#
# Attention:  1. Make sure that llvm-symbolizer binary is in PATH, or set environment variable
#                MSAN_SYMBOLIZER_PATH to point to it
#             2. Probably will produce error prone results because of non-instrumented dependencies.
#             3. Libc/libstdc++ static linking is not supported.
#
set(CMAKE_CXX_FLAGS_MEM "-fsanitize=memory -fno-omit-frame-pointer  -fsanitize-memory-track-origins=2 -fno-optimize-sibling-calls -Werror=return-type -Werror=return-type -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_LINKER_FLAGS_MEM "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=memory")
# Continue after first error. Attention: Errors after the first error might be spurious. Use with caution.
set(CMAKE_CXX_FLAGS_MEM_RECOVER "${CMAKE_CXX_FLAGS_MEM} -fsanitize-recover=all")
set(CMAKE_LINKER_FLAGS_MEM_RECOVER "${CMAKE_LINKER_FLAGS_MEM}")

# Leak and address sanitizers (which reports both memory errors and leak detection)
# Out of bounds accesses to heap, stack, global, Use after free, Use after return, Use after scope, Double-free, invalid free, Memory leaks
# Flags: https://github.com/google/sanitizers/wiki/AddressSanitizerFlags
#
# Slowdown and memory overhead of this cmake profile is reported to be:
# Slowdown:         2x
# Memory overhead:  3x
#
# Attention: 1. The Address Sanitizer maps (but does not reserve) a lot of virtual address space.
#               This means that tools like ulimit may not work as usually expected.
#            2. Probably will produce error prone results because of non-instrumented dependencies.
#            3. Libc/libstdc++ static linking is not supported.
#
set(CMAKE_CXX_FLAGS_ADD "-fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls -Werror=return-type -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_LINKER_FLAGS_ADD "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
# Continue after first error. Attention: Errors after the first error might be spurious. Use with caution.
set(CMAKE_CXX_FLAGS_ADD_RECOVER "${CMAKE_CXX_FLAGS_ADD} -fsanitize-recover=all")
set(CMAKE_LINKER_FLAGS_ADD_RECOVER "${CMAKE_LINKER_FLAGS_ADD}")

# Detects a lot of undefined behavior (see list [here](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html)).
# Flags: https://github.com/google/sanitizers/wiki/SanitizerCommonFlags
#
# Slowdown and memory overhead of this cmake profile is reported to be negligable.
set(CMAKE_CXX_FLAGS_UB "-fsanitize=undefined -Werror=return-type -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_LINKER_FLAGS_UB "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=undefined")
# Continue after first error. Attention: Errors after the first error might be spurious. Use with caution.
set(CMAKE_CXX_FLAGS_UB_NO_RECOVER "${CMAKE_CXX_FLAGS_UB} -fno-sanitize-recover=undefined")
set(CMAKE_LINKER_FLAGS_UB_NO_RECOVER "${CMAKE_LINKER_FLAGS_UB}")
