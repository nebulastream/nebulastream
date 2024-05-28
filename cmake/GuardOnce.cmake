macro(project_enable_fixguards)
    include(FetchContent)
    FetchContent_Declare(guardonce
            GIT_REPOSITORY https://github.com/cgmb/guardonce.git
            GIT_TAG        a830d4638723f9e619a1462f470fb24df1cb08dd
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
    )
    FetchContent_MakeAvailable(guardonce)

    # converts to `#pragma once` and then back to include guard, so that guarding variable is derived from file path
    add_custom_target(fix-guards
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r -e="nes-common/include/Version/version.hpp" nes-*/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -e="nes-commom/include/Version/version.hpp" -p 'path | append _ | upper' -s '\#endif// %\\n' nes-*/

        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
    message(STATUS "guardonce utility to fix include guards is available via the 'fix-guards' target")
endmacro()
