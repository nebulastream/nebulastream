# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)
project(codon-peg2cpp CXX)

find_path(PEGLIB_INCLUDE_DIR peglib.h REQUIRED)
find_package(Threads REQUIRED)
find_package(fmt CONFIG REQUIRED)

add_executable(peg2cpp "${CODON_SOURCE_PATH}/codon/util/peg2cpp.cpp")
target_compile_features(peg2cpp PRIVATE cxx_std_20)
target_include_directories(peg2cpp PRIVATE "${PEGLIB_INCLUDE_DIR}")
target_link_libraries(peg2cpp PRIVATE Threads::Threads fmt::fmt)

install(TARGETS peg2cpp RUNTIME DESTINATION tools/codon-peg2cpp)
