find_package(Git)

if (Git_FOUND)
    # Check if inside git repository
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --is-inside-work-tree
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE IS_GIT_DIRECTORY
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET OUTPUT_QUIET)
endif ()

if (IS_GIT_DIRECTORY)

    # Get last tag from git
    execute_process(COMMAND ${GIT_EXECUTABLE} describe --abbrev=0 --tags
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE ${PROJECT_NAME}_VERSION_STRING
            OUTPUT_STRIP_TRAILING_WHITESPACE)

    # Get partial versions into a list
    string(REGEX MATCHALL "-.*$|[0-9]+" ${PROJECT_NAME}_PARTIAL_VERSION_LIST
            ${${PROJECT_NAME}_VERSION_STRING})

    # Set the last released version number
    list(GET ${PROJECT_NAME}_PARTIAL_VERSION_LIST
            0 ${PROJECT_NAME}_VERSION_MAJOR)
    list(GET ${PROJECT_NAME}_PARTIAL_VERSION_LIST
            1 ${PROJECT_NAME}_VERSION_MINOR)
    list(GET ${PROJECT_NAME}_PARTIAL_VERSION_LIST
            2 ${PROJECT_NAME}_VERSION_PATCH)

    #Increment patch number to indicate next free version
    if (RELEASE_MAJOR)
        math(EXPR ${PROJECT_NAME}_VERSION_MAJOR ${${PROJECT_NAME}_VERSION_MAJOR}+1)
        set(${PROJECT_NAME}_VERSION_MINOR 0)
        set(${PROJECT_NAME}_VERSION_PATCH 0)
    elseif (RELEASE_MINOR)
        math(EXPR ${PROJECT_NAME}_VERSION_MINOR ${${PROJECT_NAME}_VERSION_MINOR}+1)
        set(${PROJECT_NAME}_VERSION_PATCH 0)
    else ()
        math(EXPR ${PROJECT_NAME}_VERSION_PATCH ${${PROJECT_NAME}_VERSION_PATCH}+1)
    endif ()

    # Get current branch name
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --abbrev-ref HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE ${PROJECT_NAME}_BRANCH_NAME
            OUTPUT_STRIP_TRAILING_WHITESPACE)

    if (${${PROJECT_NAME}_BRANCH_NAME} STREQUAL "master")
        # Set project version string
        set(${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}.${${PROJECT_NAME}_VERSION_PATCH})
    else ()
        # Get current commit SHA from git
        execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                OUTPUT_VARIABLE ${PROJECT_NAME}_VERSION_GIT_SHA
                OUTPUT_STRIP_TRAILING_WHITESPACE)

        # Set project version string with last commit hash
        set(${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}.${${PROJECT_NAME}_VERSION_PATCH}-${${PROJECT_NAME}_VERSION_GIT_SHA}-SNAPSHOT)
    endif ()

    # Unset the list
    unset(${PROJECT_NAME}_PARTIAL_VERSION_LIST)
else ()
    message(AUTHOR_WARNING "-- Git not configured. Using UNKNOWN as version")
    set(${PROJECT_NAME}_VERSION "UNKNOWN")
endif ()

message("NES Version: " ${${PROJECT_NAME}_VERSION})
