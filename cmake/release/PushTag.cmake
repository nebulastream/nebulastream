# Read the version file into variable
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp VERSION_INFO)

# Find the Major version
string(REGEX MATCH "\\NES_VERSION_MAJOR[^\n]*" MAJOR_VERSION "${VERSION_INFO}")
string(REPLACE "NES_VERSION_MAJOR" " " MAJOR_VERSION "${MAJOR_VERSION}")
string(STRIP "${MAJOR_VERSION}" MAJOR_VERSION)
message("MAJOR_VERSION ${MAJOR_VERSION}")

# Find the Minor version
string(REGEX MATCH "\\NES_VERSION_MINOR[^\n]*" MINOR_VERSION "${VERSION_INFO}")
string(REPLACE "NES_VERSION_MINOR" " " MINOR_VERSION "${MINOR_VERSION}")
string(STRIP "${MINOR_VERSION}" MINOR_VERSION)
message("MINOR_VERSION ${MINOR_VERSION}")

# Find the patch version
string(REGEX MATCH "\\NES_VERSION_PATCH[^\n]*" PATCH_VERSION "${VERSION_INFO}")
string(REPLACE "NES_VERSION_PATCH" " " PATCH_VERSION "${PATCH_VERSION}")
string(STRIP "${PATCH_VERSION}" PATCH_VERSION)
message("PATCH_VERSION ${PATCH_VERSION}")

set(${PROJECT_NAME}_VERSION ${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION})

message(INFO "-- Pushing Tag NebulaStream v${${PROJECT_NAME}_VERSION}")

# Push the version tag to repository
execute_process(COMMAND ${GIT_EXECUTABLE} tag v${${PROJECT_NAME}_VERSION} -m "GIT-CI: Releasing New Tag v${${PROJECT_NAME}_VERSION}")
execute_process(COMMAND ${GIT_EXECUTABLE} push origin v${${PROJECT_NAME}_VERSION})
