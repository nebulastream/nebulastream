# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-single-node-worker-test-util STATIC
        src/IntegrationTestUtil.cpp
)

target_include_directories(nes-single-node-worker-test-util PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_definitions(nes-single-node-worker-test-util PRIVATE
        TEST_DATA_DIR="${CMAKE_SOURCE_DIR}/${INTEGRATION_TEST_DATA_PATH}"
        SERIALIZED_QUERIES_DIR="${CMAKE_BINARY_DIR}/${INTEGRATION_TEST_DATA_PATH}/queries"
)

target_link_libraries(nes-single-node-worker-test-util PUBLIC
        nes-runtime
        nes-memory
        nes-grpc
        nes-single-node-worker-lib
        ${GTEST_LIBRARIES}
)
