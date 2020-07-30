macro(add_source PROP_NAME SOURCE_FILES)
    set(SOURCE_FILES_ABSOLUTE)
    foreach (it ${SOURCE_FILES})
        get_filename_component(ABSOLUTE_PATH ${it} ABSOLUTE)
        set(SOURCE_FILES_ABSOLUTE ${SOURCE_FILES_ABSOLUTE} ${ABSOLUTE_PATH})
    endforeach ()

    get_property(OLD_PROP_VAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set_property(GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP" ${SOURCE_FILES_ABSOLUTE} ${OLD_PROP_VAL})
endmacro()

macro(get_source PROP_NAME SOURCE_FILES)
    get_property(SOURCE_FILES_LOCAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro(add_source_nes)
    add_source(nes "${ARGN}")
endmacro()

macro(get_source_nes SOURCE_FILES)
    get_source(nes SOURCE_FILES_LOCAL)
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro(get_header_nes HEADER_FILES)
    file(GLOB_RECURSE ${HEADER_FILES} "include/*.h" "include/*.hpp")
endmacro()

find_program(CLANG_FORMAT_EXE clang-format)

macro(project_enable_clang_format)
    set(HEADER_FILES)
    set(SOURCE_FILES)
    get_header_nes(HEADER_FILES)
    get_source_nes(SOURCE_FILES)
    set(ALL_SOURCE_FILES ${SOURCE_FILES} ${HEADER_FILES})
    #message(${ALL_SOURCE_FILES})
    if (NOT ${CLANG_FORMAT_EXE} STREQUAL "CLANG_FORMAT_EXE-NOTFOUND")
        message("-- clang-format found, whole source formatting enabled through 'format' target.")
        add_custom_target(format COMMAND clang-format -style=file -i ${ALL_SOURCE_FILES})
    else ()
        message(FATAL_ERROR "clang-format is not installed.")
    endif ()
endmacro(project_enable_clang_format)

macro(project_enable_release)
    if (NOT IS_GIT_DIRECTORY)
        message(AUTHOR_WARNING " -- Disabled release target as git not configured.")
    elseif (${${PROJECT_NAME}_BRANCH_NAME} STREQUAL "master")
        message(INFO "-- Enabled release target.")
        add_custom_target(release
                COMMAND echo "Releasing NES ${${PROJECT_NAME}_VERSION}"
                )
        add_custom_command(TARGET release
                COMMAND ${GIT_EXECUTABLE} commit -am "GIT-CI: Updating NES version to ${${PROJECT_NAME}_VERSION}"
                COMMAND ${GIT_EXECUTABLE} push
                COMMENT "Updated NES version ${${PROJECT_NAME}_VERSION}")
        add_custom_command(TARGET release
                COMMAND ${GIT_EXECUTABLE} tag v${${PROJECT_NAME}_VERSION} -m "GIT-CI: Releasing New Tag v${${PROJECT_NAME}_VERSION}"
                COMMAND ${GIT_EXECUTABLE} push origin v${${PROJECT_NAME}_VERSION}
                COMMENT "Released and pushed new tag to the repository")
    else ()
        message(INFO " -- Disabled release target as currently not on master branch.")
    endif ()
endmacro(project_enable_release)

macro(project_enable_version)
    if (NOT IS_GIT_DIRECTORY)
        message(AUTHOR_WARNING " -- Disabled version target as git not configured.")
    else()
        add_custom_target(version COMMAND echo "version: ${${PROJECT_NAME}_VERSION}")
        add_custom_command(TARGET version COMMAND cp -f ${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.hpp.in ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MAJOR@/${${PROJECT_NAME}_VERSION_MAJOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_MINOR@/${${PROJECT_NAME}_VERSION_MINOR}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION_PATCH@/${${PROJECT_NAME}_VERSION_PATCH}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp
                COMMAND sed -i 's/@NES_VERSION@/${${PROJECT_NAME}_VERSION}/g' ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp)
    endif ()
endmacro(project_enable_version)