# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)
find_package(reflectcpp CONFIG REQUIRED)
add_library(image-manip-logical-functions STATIC ImageManipLogicalFunction.cpp)
target_include_directories(image-manip-logical-functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(image-manip-logical-functions PRIVATE nes-logical-operators reflectcpp::reflectcpp)
link_plugin_library(nes-logical-operators image-manip-logical-functions)
add_library(image-manip-functions STATIC ImageManipPhysicalFunction.cpp)
target_include_directories(image-manip-functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(image-manip-functions PRIVATE nes-physical-operators)
link_plugin_library(nes-physical-operators image-manip-functions)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO8_TO_JPG)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO8_TO_JPG)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO8_TO_JPG)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_MONO8)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_MONO8)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_MONO8)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_YUYV_TO_JPG)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_YUYV_TO_JPG)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_YUYV_TO_JPG)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_PNG16)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_PNG16)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_PNG16)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_JPG)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_JPG)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_JPG)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_YUYV)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_YUYV)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_YUYV)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO8_TO_YUYV)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO8_TO_YUYV)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO8_TO_YUYV)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_FACE_DETECTION)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_FACE_DETECTION)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_FACE_DETECTION)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_SERIALIZE)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_SERIALIZE)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_SERIALIZE)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_DESERIALIZE)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_DESERIALIZE)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_DESERIALIZE)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_DRAW_RECTANGLE)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_DRAW_RECTANGLE)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_DRAW_RECTANGLE)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_ROI)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_ROI)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_ROI)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_AVG)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_AVG)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_AVG)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_CELSIUS)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_CELSIUS)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_TO_CELSIUS)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_MAX)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_MAX)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_MAX)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_MIN)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MONO16_MIN)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MONO16_MIN)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_RECTANGLE)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_RECTANGLE)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_RECTANGLE)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_AUDIO_TO_MFCC)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_AUDIO_TO_MFCC)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_AUDIO_TO_MFCC)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_ARGMAX_F32)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_ARGMAX_F32)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_ARGMAX_F32)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MAX_F32)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MAX_F32)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MAX_F32)
add_registry_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MAX_ABS_F32)
add_unreflection_entry(LogicalFunction ImageManip KEY IMAGE_MANIP_MAX_ABS_F32)
add_registry_entry(PhysicalFunction ImageManip KEY IMAGE_MANIP_MAX_ABS_F32)

find_package(PkgConfig REQUIRED)
pkg_check_modules(aravis REQUIRED IMPORTED_TARGET GLOBAL aravis-0.8)
find_package(OpenSSL CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)

include(FetchContent)
set(B_PRODUCTION_MODE ON CACHE BOOL "Disable battery::embed development features" FORCE)
FetchContent_Declare(
        battery-embed
        GIT_REPOSITORY https://github.com/batterycenter/embed.git
        GIT_TAG v1.2.19
        GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(battery-embed)
b_embed(image-manip-functions Cascade.xml)

configure_file(calibration.bin calibration.bin COPYONLY)
target_compile_definitions(image-manip-functions PRIVATE IMAGE_MANIP_RESOURCE_DIR="${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(image-manip-functions
        PUBLIC image-manip-logical-functions
        PRIVATE opencv_core opencv_imgproc opencv_objdetect opencv_imgcodecs OpenSSL::Crypto PkgConfig::aravis)
