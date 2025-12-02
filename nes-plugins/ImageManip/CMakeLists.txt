# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin("ImageManipMono8ToJPG" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToMono8" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipToBase64" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipDrawRectangle" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipFaceDetection" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipYUYVToJPG" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToPNG16" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16ROI" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16AVG" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16MAX" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono8ToYUYV" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16MIN" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToCelsius" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipSerialize" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipDeserialize" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipRectangle" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64ToTensor" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)

add_plugin_as_library("ImageManipDrawRectangle" PhysicalFunction nes-physical-operators image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono8ToJPG" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToMono8" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipToBase64" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipFaceDetection" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipYUYVToJPG" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToPNG16" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16ROI" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16AVG" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16MAX" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono8ToYUYV" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16MIN" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipMono16ToCelsius" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipSerialize" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipDeserialize" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipRectangle" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64ToTensor" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)


include(FetchContent)
SET(B_PRODUCTION_MODE ON)
FetchContent_Declare(
        battery-embed
        GIT_REPOSITORY https://github.com/batterycenter/embed.git
        GIT_TAG v1.2.19
)
FetchContent_MakeAvailable(battery-embed)

find_package(PkgConfig)
pkg_check_modules(aravis REQUIRED IMPORTED_TARGET GLOBAL aravis-0.8)

find_package(OpenSSL CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)
target_link_libraries(image-manip-functions PRIVATE opencv_imgproc OpenSSL::SSL opencv_objdetect opencv_imgcodecs PkgConfig::aravis)
target_include_directories(image-manip-functions PRIVATE ${OpenCV_INCLUDE_DIRS})
b_embed(image-manip-functions Cascade.xml)
b_embed(image-manip-functions calibration.bin)
