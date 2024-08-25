# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generate protobuf and grpc files and add them to individual nes-grpc target
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
add_library(nes-grpc ${PROTO_FILES})
target_include_directories(nes-grpc PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(
        TARGET nes-grpc
        IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
        PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")

protobuf_generate(
        TARGET nes-grpc
        IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
        LANGUAGE grpc
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
        PLUGIN protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
)

target_link_libraries(nes-grpc PUBLIC ${Protobuf_LIBRARIES} gRPC::grpc gRPC::grpc++)
