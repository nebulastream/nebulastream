# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)
get_source_nes_client(nes_client_SOURCE_FILES)
get_header_nes_client(nes_client_HEADER_FILES)

message(STATUS "Client Headers: ${nes_client_HEADER_FILES}")
message(STATUS "Client Source: ${nes_client_SOURCE_FILES}")

# Add Library
add_library(nes_client SHARED ${nes_client_SOURCE_FILES} ${nes_client_HEADER_FILES})
target_include_directories(nes_client PUBLIC "include")
#target_include_directories(nes_client PUBLIC "../" "../include")
target_link_libraries(nes_client PRIVATE nes)

#Change to alter behaviour of cpack and installation directory of deb package
install(TARGETS nes_client
        COMPONENT nesLibrary
        DESTINATION lib)

SET(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_VERSION ${${PROJECT_NAME}_VERSION})
set(CPACK_PACKAGE_NAME "NebulaStream")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Team NebulaStream")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Team NebulaStream") #required
set(CPACK_PACKAGE_VENDOR "Team NebulaStream")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_COMPONENTS_ALL nesExecutables nesLibrary nesHeaders)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "cmake, libdwarf-dev, libdwarf1, binutils-dev, libdw-dev, libssl-dev")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")

##This allows call to ldconfig to load the configuration after installation
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_LIST_DIR}/triggers")
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)
INCLUDE(CPack)