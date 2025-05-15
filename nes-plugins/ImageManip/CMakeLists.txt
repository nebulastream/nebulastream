# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin("ImageManipMono16ToGray" LogicalFunction nes-functions NodeFunctionImageManip.cpp)
add_plugin("ImageManipToBase64" LogicalFunction nes-functions NodeFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64" LogicalFunction nes-functions NodeFunctionImageManip.cpp)
add_plugin("ImageManipDrawRectangle" LogicalFunction nes-functions NodeFunctionImageManip.cpp)
add_plugin("ImageManipFaceDetection" LogicalFunction nes-functions NodeFunctionImageManip.cpp)


add_plugin_as_library("ImageManipDrawRectangle" ExecutableFunction nes-execution-registry image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToGray" ExecutableFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipToBase64" ExecutableFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64" ExecutableFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipFaceDetection" ExecutableFunction image-manip-functions ExecutableFunctionImageManip.cpp)

cmake_minimum_required(VERSION 3.21)
project(Test)

include(FetchContent)
SET(B_PRODUCTION_MODE ON)
FetchContent_Declare(
        battery-embed
        GIT_REPOSITORY https://github.com/batterycenter/embed.git
        GIT_TAG        v1.2.19
)
FetchContent_MakeAvailable(battery-embed)


find_package(OpenCV CONFIG REQUIRED)
target_link_libraries(image-manip-functions PRIVATE opencv_imgproc opencv_objdetect)
target_include_directories(image-manip-functions PRIVATE ${OpenCV_INCLUDE_DIRS})
b_embed(image-manip-functions Cascade.xml)
