# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0

add_plugin_as_library(ImageManipDrawRectangle LogicalFunction image-manip-logical-functions ImageManipLogicalFunction.cpp)
add_plugin(ImageManipMono8ToJPG LogicalFunction)
add_plugin(ImageManipMono16ToMono8 LogicalFunction)
add_plugin(ImageManipFaceDetection LogicalFunction)
add_plugin(ImageManipYUYVToJPG LogicalFunction)
add_plugin(ImageManipMono16ToPNG16 LogicalFunction)
add_plugin(ImageManipMono16ROI LogicalFunction)
add_plugin(ImageManipMono16AVG LogicalFunction)
add_plugin(ImageManipMono16MAX LogicalFunction)
add_plugin(ImageManipMono8ToYUYV LogicalFunction)
add_plugin(ImageManipMono16MIN LogicalFunction)
add_plugin(ImageManipMono16ToCelsius LogicalFunction)
add_plugin(ImageManipSerialize LogicalFunction)
add_plugin(ImageManipDeserialize LogicalFunction)
add_plugin(ImageManipRectangle LogicalFunction)

add_plugin_as_library(ImageManipDrawRectangle PhysicalFunction image-manip-functions ImageManipPhysicalFunction.cpp)
add_plugin(ImageManipMono8ToJPG PhysicalFunction)
add_plugin(ImageManipMono16ToMono8 PhysicalFunction)
add_plugin(ImageManipFaceDetection PhysicalFunction)
add_plugin(ImageManipYUYVToJPG PhysicalFunction)
add_plugin(ImageManipMono16ToPNG16 PhysicalFunction)
add_plugin(ImageManipMono16ROI PhysicalFunction)
add_plugin(ImageManipMono16AVG PhysicalFunction)
add_plugin(ImageManipMono16MAX PhysicalFunction)
add_plugin(ImageManipMono8ToYUYV PhysicalFunction)
add_plugin(ImageManipMono16MIN PhysicalFunction)
add_plugin(ImageManipMono16ToCelsius PhysicalFunction)
add_plugin(ImageManipSerialize PhysicalFunction)
add_plugin(ImageManipDeserialize PhysicalFunction)
add_plugin(ImageManipRectangle PhysicalFunction)

find_package(PkgConfig REQUIRED)
pkg_check_modules(aravis REQUIRED IMPORTED_TARGET GLOBAL aravis-0.8)
find_package(OpenSSL CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)

configure_file(Cascade.xml Cascade.xml COPYONLY)
configure_file(calibration.bin calibration.bin COPYONLY)
target_compile_definitions(image-manip-functions PRIVATE IMAGE_MANIP_RESOURCE_DIR="${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(image-manip-functions PRIVATE opencv_core opencv_imgproc opencv_objdetect opencv_imgcodecs OpenSSL::Crypto PkgConfig::aravis)
