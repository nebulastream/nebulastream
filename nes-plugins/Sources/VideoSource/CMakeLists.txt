# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

add_plugin_as_library(Video Source video_source_plugin_library VideoSource.cpp)
add_plugin_as_library(Video SourceValidation video_source_validation_plugin_library VideoSource.cpp)

target_include_directories(video_source_plugin_library PRIVATE .)
target_include_directories(video_source_validation_plugin_library PRIVATE .)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ARAVIS REQUIRED IMPORTED_TARGET aravis-0.8)
target_link_libraries(video_source_plugin_library PRIVATE PkgConfig::ARAVIS)
target_link_libraries(video_source_validation_plugin_library PRIVATE PkgConfig::ARAVIS)
