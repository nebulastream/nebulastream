# Read the version file into variable
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp VERSION_INFO)

# Find the Major version
string(REGEX MATCH "\\NES_VERSION_MAJOR[^\n]*" MAJOR_VERSION "${VERSION_INFO}")
string(REPLACE "NES_VERSION_MAJOR" " " MAJOR_VERSION "${MAJOR_VERSION}")
string(STRIP "${MAJOR_VERSION}" MAJOR_VERSION)
message("MAJOR_VERSION ${MAJOR_VERSION}")

#Find the Minor version
string(REGEX MATCH "\\NES_VERSION_MINOR[^\n]*" MINOR_VERSION "${VERSION_INFO}")
string(REPLACE "NES_VERSION_MINOR" " " MINOR_VERSION "${MINOR_VERSION}")
string(STRIP "${MINOR_VERSION}" MINOR_VERSION)
message("MINOR_VERSION ${MINOR_VERSION}")

#Find the patch version
string(REGEX MATCH "\\NES_VERSION_PATCH[^\n]*" PATCH_VERSION "${VERSION_INFO}")
string(REPLACE "NES_VERSION_PATCH" " " PATCH_VERSION "${PATCH_VERSION}")
string(STRIP "${PATCH_VERSION}" PATCH_VERSION)
message("PATCH_VERSION ${PATCH_VERSION}")

unset(POSTFIX_VERSION)

#Increment patch number to indicate next free version
if (MAJOR_RELEASE)
    math(EXPR MAJOR_VERSION ${MAJOR_VERSION}+1)
    set(MINOR_VERSION 0)
    set(PATCH_VERSION 0)
elseif (MINOR_RELEASE)
    math(EXPR MINOR_VERSION ${MINOR_VERSION}+1)
    set(PATCH_VERSION 0)
elseif (POST_RELEASE)
    math(EXPR PATCH_VERSION ${PATCH_VERSION}+1)
    set(POSTFIX_VERSION -SNAPSHOT)
endif ()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/semver/version.hpp.in
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp COPYONLY)

# Update version file with new version
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_MAJOR@/${MAJOR_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp")
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_MINOR@/${MINOR_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp")
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_PATCH@/${PATCH_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp")
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_POST_FIX@/${POSTFIX_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include/Version/version.hpp")

set(${PROJECT_NAME}_VERSION ${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}${POSTFIX_VERSION})

execute_process(COMMAND ${GIT_EXECUTABLE} commit -am "GIT-CI: Updating NES version to ${${PROJECT_NAME}_VERSION}")
execute_process(COMMAND ${GIT_EXECUTABLE} push origin)

message(INFO "-- Releasing NebulaStream v${${PROJECT_NAME}_VERSION}")