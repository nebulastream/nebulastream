# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

add_library(nes-codon-plugin STATIC src/Registry.cpp)
set_target_properties(nes-codon-plugin PROPERTIES EXPORT_NAME SDK)
target_include_directories(nes-codon-plugin PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(nes-codon-plugin PRIVATE ${CMAKE_DL_LIBS})
add_library(NESCodonPlugin::SDK ALIAS nes-codon-plugin)

configure_package_config_file(
    cmake/NESCodonPluginConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/NESCodonPluginConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/NESCodonPlugin")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/NESCodonPluginConfigVersion.cmake"
    VERSION 1.0.0
    COMPATIBILITY SameMajorVersion)

install(TARGETS nes-codon-plugin EXPORT NESCodonPluginTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(EXPORT NESCodonPluginTargets NAMESPACE NESCodonPlugin:: DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/NESCodonPlugin")
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/NESCodonPluginConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/NESCodonPluginConfigVersion.cmake"
    cmake/NESCodonPlugin.cmake
    cmake/NESCodonPluginDescriptor.cpp.in
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/NESCodonPlugin")
