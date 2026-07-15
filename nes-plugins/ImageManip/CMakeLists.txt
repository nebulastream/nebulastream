# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0

add_plugin_as_library(image_manip_draw_rectangle LogicalFunction image-manip-logical-functions ImageManipLogicalFunction.cpp)
add_plugin(image_manip_mono8_to_jpg LogicalFunction)
add_plugin(image_manip_mono16_to_mono8 LogicalFunction)
add_plugin(image_manip_face_detection LogicalFunction)
add_plugin(image_manip_yuyv_to_jpg LogicalFunction)
add_plugin(image_manip_mono16_to_png16 LogicalFunction)
add_plugin(image_manip_mono16_roi LogicalFunction)
add_plugin(image_manip_mono16_avg LogicalFunction)
add_plugin(image_manip_mono16_max LogicalFunction)
add_plugin(image_manip_mono8_to_yuyv LogicalFunction)
add_plugin(image_manip_mono16_min LogicalFunction)
add_plugin(image_manip_mono16_to_celsius LogicalFunction)
add_plugin(image_manip_serialize LogicalFunction)
add_plugin(image_manip_deserialize LogicalFunction)
add_plugin(image_manip_rectangle LogicalFunction)

add_plugin_as_library(image_manip_draw_rectangle PhysicalFunction image-manip-functions ImageManipPhysicalFunction.cpp)
add_plugin(image_manip_mono8_to_jpg PhysicalFunction)
add_plugin(image_manip_mono16_to_mono8 PhysicalFunction)
add_plugin(image_manip_face_detection PhysicalFunction)
add_plugin(image_manip_yuyv_to_jpg PhysicalFunction)
add_plugin(image_manip_mono16_to_png16 PhysicalFunction)
add_plugin(image_manip_mono16_roi PhysicalFunction)
add_plugin(image_manip_mono16_avg PhysicalFunction)
add_plugin(image_manip_mono16_max PhysicalFunction)
add_plugin(image_manip_mono8_to_yuyv PhysicalFunction)
add_plugin(image_manip_mono16_min PhysicalFunction)
add_plugin(image_manip_mono16_to_celsius PhysicalFunction)
add_plugin(image_manip_serialize PhysicalFunction)
add_plugin(image_manip_deserialize PhysicalFunction)
add_plugin(image_manip_rectangle PhysicalFunction)

find_package(PkgConfig REQUIRED)
pkg_check_modules(aravis REQUIRED IMPORTED_TARGET GLOBAL aravis-0.8)
find_package(OpenSSL CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)

configure_file(Cascade.xml Cascade.xml COPYONLY)
configure_file(calibration.bin calibration.bin COPYONLY)
target_compile_definitions(image-manip-functions PRIVATE IMAGE_MANIP_RESOURCE_DIR="${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(image-manip-functions PRIVATE opencv_core opencv_imgproc opencv_objdetect opencv_imgcodecs OpenSSL::Crypto PkgConfig::aravis)
