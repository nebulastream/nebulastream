# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Enable ONNX Plugin")

find_package(unofficial-nsync CONFIG REQUIRED)
set(LIBRARIES ${LIBRARIES} unofficial::nsync::nsync_cpp)

find_package(ONNX CONFIG REQUIRED)
set(LIBRARIES ${LIBRARIES} onnx)

find_package(unofficial-cpuinfo CONFIG REQUIRED)
set(LIBRARIES ${LIBRARIES} unofficial::cpuinfo::cpuinfo)

add_library(onnxruntime::common UNKNOWN IMPORTED)
set_target_properties(onnxruntime::common PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_common.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_common.a)

add_library(onnxruntime::flatbuffers UNKNOWN IMPORTED)
set_target_properties(onnxruntime::flatbuffers PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_flatbuffers.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_flatbuffers.a)

add_library(onnxruntime::framework UNKNOWN IMPORTED)
set_target_properties(onnxruntime::framework PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_framework.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_framework.a)

add_library(onnxruntime::graph UNKNOWN IMPORTED)
set_target_properties(onnxruntime::graph PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_graph.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_graph.a)

add_library(onnxruntime::mlas UNKNOWN IMPORTED)
set_target_properties(onnxruntime::mlas PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_mlas.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_mlas.a)

add_library(onnxruntime::optimizer UNKNOWN IMPORTED)
set_target_properties(onnxruntime::optimizer PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_optimizer.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_optimizer.a)

add_library(onnxruntime::providers UNKNOWN IMPORTED)
set_target_properties(onnxruntime::providers PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_providers.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_providers.a)

add_library(onnxruntime::session UNKNOWN IMPORTED)
set_target_properties(onnxruntime::session PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_session.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_session.a)

add_library(onnxruntime::util UNKNOWN IMPORTED)
set_target_properties(onnxruntime::util PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include/onnxruntime
        IMPORTED_LOCATION ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/libonnxruntime_util.a
        IMPORTED_LOCATION_DEBUG ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/lib/libonnxruntime_util.a)

add_library(onnxruntime::onnxruntime INTERFACE IMPORTED)
set_property(TARGET onnxruntime::onnxruntime PROPERTY
        INTERFACE_LINK_LIBRARIES
        onnxruntime::common
        onnxruntime::flatbuffers
        onnxruntime::framework
        onnxruntime::graph
        onnxruntime::mlas
        onnxruntime::optimizer
        onnxruntime::providers
        onnxruntime::session
        onnxruntime::util
        )


add_library(nes-onnx SHARED
        src/Execution/Operators/ONNX/ONNXInferenceOperatorHandler.cpp
        src/Execution/Operators/ONNX/ONNXInferenceOperator.cpp
        src/Execution/Operators/ONNX/ONNXAdapter.cpp
        src/Execution/Operators/ONNX/ONNXOperatorLoweringPlugin.cpp
        )
target_link_libraries(nes-onnx PRIVATE nes onnxruntime::onnxruntime)
add_dependencies(build_all_plugins nes-onnx)

target_include_directories(nes-onnx PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_target_properties(nes-onnx PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/nes-plugins/)


if (NES_ENABLES_TESTS)
    add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}/tests/testData/")
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes-onnx
        EXPORT NebulaStreamTargets
        LIBRARY
        DESTINATION ${CMAKE_INSTALL_BINDIR}/nes-plugins
        COMPONENT nesLibrary)

# Include onnx library in the deb package and install it in the lib folder
#install(IMPORTED_RUNTIME_ARTIFACTS onnxruntime::onnxruntime DESTINATION ${CMAKE_INSTALL_BINDIR}/nes-plugins)

