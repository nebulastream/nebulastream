# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The Python UDF bridge is an optional runtime artifact (a .so users dlopen via CREATE FUNCTION),
# not linked into the engine. It embeds CPython, so it only builds where the Python "embed"
# development files are available.
find_package(Python3 COMPONENTS Development.Embed QUIET)
if (TARGET Python3::Python)
    add_library(nes-python-udf-bridge SHARED PythonUdfBridge.cpp)
    target_include_directories(nes-python-udf-bridge PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_link_libraries(nes-python-udf-bridge PRIVATE Python3::Python)
    # The systest binary (configured before this dir) dlopens the bridge; ensure it is built alongside.
    if (TARGET systest)
        add_dependencies(systest nes-python-udf-bridge)
    endif ()
    # UdfBridgeRegistry resolves LANGUAGE clauses to <worker executable dir>/nes-udf-bridges/<file> at
    # runtime, so the worker needs the bridge built alongside it too, and installed to that fixed
    # relative location (see the install() rule below). The nes-worker Docker image (see
    # cmake/PackageDockerImages.cmake) COPYs this same build-tree-relative nes-udf-bridges/ directory,
    # so stage the bridge there via symlink, mirroring the systest staging above.
    if (TARGET nes-single-node-worker)
        add_dependencies(nes-single-node-worker nes-python-udf-bridge)
        add_custom_command(TARGET nes-single-node-worker POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:nes-single-node-worker>/nes-udf-bridges"
                COMMAND ${CMAKE_COMMAND} -E create_symlink "$<TARGET_FILE:nes-python-udf-bridge>" "$<TARGET_FILE_DIR:nes-single-node-worker>/nes-udf-bridges/$<TARGET_FILE_NAME:nes-python-udf-bridge>")
    endif ()
    install(TARGETS nes-python-udf-bridge
            RUNTIME DESTINATION bin/nes-udf-bridges
            LIBRARY DESTINATION bin/nes-udf-bridges)
    message(STATUS "Python UDF bridge enabled (Python ${Python3_VERSION})")
else ()
    message(STATUS "Python UDF bridge disabled (Python Development.Embed not found)")
endif ()
