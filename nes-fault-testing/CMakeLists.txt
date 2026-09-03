# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_subdirectory(src)
get_source(nes-fault-testing NES_FAULT_TESTING_SOURCE_FILES)

add_library(nes-fault-testing ${NES_FAULT_TESTING_SOURCE_FILES})
target_link_libraries(nes-fault-testing PUBLIC nes-common)
target_include_directories(nes-fault-testing
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-fault-testing REUSE_FROM nes-common)
    target_compile_options(nes-fault-testing PUBLIC "-fPIC")
endif ()

add_executable(nes-fault-testing-runner ${CMAKE_CURRENT_SOURCE_DIR}/src/FaultTestingRunner.cpp)
set_property(TARGET nes-fault-testing-runner PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(nes-fault-testing-runner PRIVATE nes-fault-testing nes-systest-lib)