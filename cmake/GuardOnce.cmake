# TODO: make it run only on changed and new header files. The changed header files
# can be obtained via
# git ls-files ${CMAKE_CURRENT_SOURCE_DIR}/include/ -m
# and
# git ls-files ${CMAKE_CURRENT_SOURCE_DIR}/include/ --others --exclude-standard
# somehow the output of git-ls has to redirected to guard2once, it does not read
# input from files. One possible workaround could be write the output of a custom
# command to a file which is then read in a string and passed to guard2once?
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
            COMMAND python3 -m guardonce.guard2once -r nes-benchmark/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-benchmark/include/

            COMMAND python3 -m guardonce.guard2once -r nes-catalogs/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-catalogs/include/

            COMMAND python3 -m guardonce.guard2once -r nes-client/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-client/include/

            COMMAND python3 -m guardonce.guard2once -r -e="*/include/Version/version.hpp" nes-common/include/
            COMMAND python3 -m guardonce.once2guard -r -e="*/include/Version/version.hpp" -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-common/include/

            COMMAND python3 -m guardonce.guard2once -r nes-compiler/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-compiler/include/

            COMMAND python3 -m guardonce.guard2once -r nes-configurations/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-configurations/include/

            COMMAND python3 -m guardonce.guard2once -r nes-coordinator/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-coordinator/include/

            COMMAND python3 -m guardonce.guard2once -r nes-coordinator/tests/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-coordinator/tests/include/

            COMMAND python3 -m guardonce.guard2once -r nes-data-types/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-data-types/include/

            COMMAND python3 -m guardonce.guard2once -r nes-operators/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-operators/include/

            COMMAND python3 -m guardonce.guard2once -r nes-optimizer/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-optimizer/include/

            COMMAND python3 -m guardonce.guard2once -r nes-runtime/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-runtime/include/

            COMMAND python3 -m guardonce.guard2once -r nes-worker/include/
            COMMAND python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-worker/include/

            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            )
    message(" -- guardonce utility to fix ifdefs is available via the 'fix-guards' target")
endmacro(project_enable_fixguards)