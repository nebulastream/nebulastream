# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(Integration)
add_subdirectory(Util)

if (MOLD_EXECUTABLE AND NES_USE_MOLD_IF_AVAILABLE)
    # c.f. issue 294-P, and ../CMakeLists.txt
    # This is a smaller example of the mold-strip bug that might aid further bug minification
    add_executable(mold-strip-bug-small MoldStripBug.cpp)
    target_compile_options(mold-strip-bug-small PRIVATE "-Wno-unused-parameter")
    target_link_libraries(mold-strip-bug-small PRIVATE nes-execution)

    add_custom_command(
        OUTPUT mold-strip-bug-small_stripped
        COMMAND llvm-strip-18 -o mold-strip-bug-small_stripped $<TARGET_FILE:mold-strip-bug-small>
        DEPENDS mold-strip-bug-small
        COMMENT "Breaking mold-strip-bug by stripping with llvm-strip-18"
    )

    add_custom_target(mold-strip-bug-small_broken ALL DEPENDS mold-strip-bug-small_stripped)

    add_test(NAME mold-strip-bug-small        COMMAND mold-strip-bug-small)
    # Expects the program to fail by failing if the program exits correctly
    add_test(NAME mold-strip-bug-small_broken COMMAND sh -c "./mold-strip-bug-small_stripped && exit 1 || exit 0")
endif ()
