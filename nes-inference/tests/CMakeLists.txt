# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_inference_test)
    # Define the argument structure
    set(options "")  # No flag options in this case
    set(oneValueArgs TEST_NAME TEST_DATA)  # Single value arguments
    set(multiValueArgs SOURCES)  # Multi-value arguments

    # Parse the arguments
    cmake_parse_arguments(
            ARG  # Prefix for parsed variables
            "${options}"
            "${oneValueArgs}"
            "${multiValueArgs}"
            ${ARGN}  # All arguments passed to the function
    )

    add_nes_test_nebuli(${ARG_TEST_NAME} ${ARG_SOURCES})
    set(TARGET_NAME ${ARG_TEST_NAME})
    target_link_libraries(${TARGET_NAME} nes-inference Boost::system Boost::filesystem nes-common nes-data-types nes-grpc)
    target_include_directories(${TARGET_NAME} PRIVATE .)

    if (ARG_TEST_DATA)
        include(ExternalData)
        set(ExternalData_URL_TEMPLATES
                "https://github.com/nebulastream/nes-datasets/releases/download/v1/%(algo)_%(hash)"
        )
        set(ExternalData_TIMEOUT_ABSOLUTE 600)
        ExternalData_Expand_Arguments(
                TEST_DATA
                TEST_DATA_PATH
                ${ARG_TEST_DATA}
        )
        target_compile_definitions(${TARGET_NAME} PRIVATE "TEST_DATA_DIR=\"${TEST_DATA_PATH}\"")
        add_dependencies(${TARGET_NAME} TEST_DATA)
    endif ()
endfunction()

add_nes_inference_test(
        TEST_NAME ModelLoaderTest
        SOURCES ModelLoaderTest.cpp
        TEST_DATA DATA{testdata/,REGEX:.*}
)
