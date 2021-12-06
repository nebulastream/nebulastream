# TODO: make it run only on changed and new header files. The changed header files can be obtained via
# git ls-files ${CMAKE_CURRENT_SOURCE_DIR}/include/ -m
# and
# git ls-files ${CMAKE_CURRENT_SOURCE_DIR}/include/ --others --exclude-standard
# somehow the output of git-ls has to redirected to guard2once, it does not read
# input from files. One possible workaround could be write the output of a custom command to a file
# which is then read in a string and passed to guard2once?
macro(project_enable_fixguards)
    include(FetchContent)
    FetchContent_Declare(guardonce
            GIT_REPOSITORY https://github.com/cgmb/guardonce.git
	    GIT_TAG        a830d4638723f9e619a1462f470fb24df1cb08dd
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            )
    FetchContent_MakeAvailable(guardonce)
    set(ENV{PYTHONPATH} "${guardonce_SOURCE_DIR}/guardonce")

    add_custom_target(fix-guards
            COMMAND python3 -m guardonce.guard2once -r include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | prepend nes_ | append _ | upper' -s '\#endif  // %\\n' include/
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            )
    message(" -- guardonce utility to fix ifdefs is available via the 'fix-guards' target")
endmacro(project_enable_fixguards)
