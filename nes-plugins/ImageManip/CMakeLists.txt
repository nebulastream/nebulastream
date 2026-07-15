# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0

add_plugin_as_library(IMAGE_MANIP_DRAW_RECTANGLE LogicalFunction image-manip-logical-functions ImageManipLogicalFunction.cpp)
add_plugin(IMAGE_MANIP_MONO8_TO_JPG LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_MONO8 LogicalFunction)
add_plugin(IMAGE_MANIP_FACE_DETECTION LogicalFunction)
add_plugin(IMAGE_MANIP_YUYV_TO_JPG LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_PNG16 LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_JPG LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_ROI LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_AVG LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_MAX LogicalFunction)
add_plugin(IMAGE_MANIP_MONO8_TO_YUYV LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_MIN LogicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_CELSIUS LogicalFunction)
add_plugin(IMAGE_MANIP_SERIALIZE LogicalFunction)
add_plugin(IMAGE_MANIP_DESERIALIZE LogicalFunction)
add_plugin(IMAGE_MANIP_RECTANGLE LogicalFunction)

add_plugin_as_library(IMAGE_MANIP_DRAW_RECTANGLE PhysicalFunction image-manip-functions ImageManipPhysicalFunction.cpp)
add_plugin(IMAGE_MANIP_MONO8_TO_JPG PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_MONO8 PhysicalFunction)
add_plugin(IMAGE_MANIP_FACE_DETECTION PhysicalFunction)
add_plugin(IMAGE_MANIP_YUYV_TO_JPG PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_PNG16 PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_JPG PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_ROI PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_AVG PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_MAX PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO8_TO_YUYV PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_MIN PhysicalFunction)
add_plugin(IMAGE_MANIP_MONO16_TO_CELSIUS PhysicalFunction)
add_plugin(IMAGE_MANIP_SERIALIZE PhysicalFunction)
add_plugin(IMAGE_MANIP_DESERIALIZE PhysicalFunction)
add_plugin(IMAGE_MANIP_RECTANGLE PhysicalFunction)

find_package(PkgConfig REQUIRED)
pkg_check_modules(aravis REQUIRED IMPORTED_TARGET GLOBAL aravis-0.8)
find_package(OpenSSL CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)

configure_file(Cascade.xml Cascade.xml COPYONLY)
configure_file(calibration.bin calibration.bin COPYONLY)
target_compile_definitions(image-manip-functions PRIVATE IMAGE_MANIP_RESOURCE_DIR="${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(image-manip-functions PRIVATE opencv_core opencv_imgproc opencv_objdetect opencv_imgcodecs OpenSSL::Crypto PkgConfig::aravis)
