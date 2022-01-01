# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Add Source Code
add_subdirectory(src)

# Add Library
get_source_nes(nes_SOURCE_FILES)
get_header_nes(nes_HEADER_FILES)
add_library(nes SHARED ${nes_SOURCE_FILES} ${nes_HEADER_FILES} ${OUTPUT_SOURCES} ${OUTPUT_HEADERS})
target_include_directories(nes PUBLIC "include")
target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/../")
target_link_libraries(nes PUBLIC ${LIBRARIES} ${PROTOBUF_LIBRARIES})

if (GRPC_LIBRARIES)
    target_link_libraries(nes PRIVATE ${GRPC_LIBRARIES})
endif ()

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PRIVATE ${LIBRARIES} nes)
target_include_directories(nesCoordinator PUBLIC "include")
target_include_directories(nesCoordinator PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesCoordinator PRIVATE ${GRPC_LIBRARIES})
endif ()

add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC ${LIBRARIES} nes)
target_include_directories(nesWorker PUBLIC "include")
target_include_directories(nesWorker PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesWorker PRIVATE ${GRPC_LIBRARIES})
endif ()

add_subdirectory(tests)
