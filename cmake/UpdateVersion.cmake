
message("lalaa :  ${MAJOR_VERSION}")

#Increment patch number to indicate next free version
if (${RELEASE} STREQUAL "MAJOR")
    message(INFO "Called ${RELEASE}")
    math(EXPR MAJOR_VERSION ${MAJOR_VERSION}+1)
    set(MINOR_VERSION 0)
    set(PATCH_VERSION 0)
elseif (${RELEASE} STREQUAL "MINOR")
    message(INFO "Called ${RELEASE}")
    math(EXPR MINOR_VERSION ${MINOR_VERSION}+1)
    set(PATCH_VERSION 0)
elseif (${RELEASE} STREQUAL "PATCH")
    message(INFO "Called ${RELEASE}")
    math(EXPR PATCH_VERSION ${PATCH_VERSION}+1)
endif ()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/version.hpp.in
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include/Version/version.hpp COPYONLY)

# Update version file with new version
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_MAJOR@/${MAJOR_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include/Version/version.hpp")
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_MINOR@/${MINOR_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include/Version/version.hpp")
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_PATCH@/${PATCH_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include/Version/version.hpp")
execute_process(COMMAND bash -c "sed -i 's/@NES_VERSION_POST_FIX@//g' ${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include/Version/version.hpp")

set(${PROJECT_NAME}_VERSION ${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION})

message(INFO "Called ${${PROJECT_NAME}_VERSION}")