# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

add_plugin_as_library(Audio Source audio_source_plugin_library AudioSource.cpp)
add_plugin_as_library(Audio SourceValidation audio_source_validation_plugin_library AudioSource.cpp)

target_include_directories(audio_source_plugin_library PRIVATE .)
target_include_directories(audio_source_validation_plugin_library PRIVATE .)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ALSA REQUIRED IMPORTED_TARGET alsa)
target_link_libraries(audio_source_plugin_library PRIVATE PkgConfig::ALSA)
target_link_libraries(audio_source_validation_plugin_library PRIVATE PkgConfig::ALSA)
