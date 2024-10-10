include(GoogleTest)
# Target to build all integration tests
add_custom_target(integration_tests)
# Target to build all e2e tests
add_custom_target(e2e_tests)


if (NES_COMPUTE_COVERAGE)
    # Define targets that we call in later targets
    add_custom_target(list_nondefault_profraw
            COMMAND find "${CMAKE_CURRENT_BINARY_DIR}" -name '*.profraw' -printf '%P\\n' | grep -v --exclude='profraw_files.csv' '.*default.profraw' > ${CMAKE_BINARY_DIR}/profraw_files.csv
            BYPRODUCTS profraw_files.csv
    )
    add_custom_target(
            merge_nondefault_profraw
            COMMAND llvm-profdata-${LLVM_VERSION_MAJOR} merge -f ${CMAKE_BINARY_DIR}/profraw_files.csv --output combined_nondefault.profdata
            BYPRODUCTS combined_nondefault.profdata
            DEPENDS list_nondefault_profraw
    )

    # Add all targets to be covered by code coverage
    file(GLOB NES_FOLDER_NAMES_COMMA_SEPARATED "${CMAKE_CURRENT_SOURCE_DIR}/nes-*")
    list(REMOVE_ITEM NES_FOLDER_NAMES_COMMA_SEPARATED "${CMAKE_CURRENT_SOURCE_DIR}/nes-nebuli" "${CMAKE_CURRENT_SOURCE_DIR}/nes-single-node-worker")

    message(STATUS "Code coverage folders: ${NES_FOLDER_NAMES_COMMA_SEPARATED}")
    set(code_covered_targets)
    # Iterate over each target name and add the paths to the each target file
    foreach (TARGET ${NES_FOLDER_NAMES_COMMA_SEPARATED})
        get_filename_component(NES_FOLDER_NAME ${TARGET} NAME)
        list(APPEND code_covered_targets $<TARGET_FILE:${NES_FOLDER_NAME}>)
    endforeach ()
    list(APPEND code_covered_targets $<TARGET_FILE:nebuli> $<TARGET_FILE:single-node>)
    message(STATUS "Code covered targets: ${code_covered_targets}")


    # format for binaries in llvm-cov does not use --object for the first BIN
    set(code_covered_binary_args $<JOIN:${code_covered_targets}, --object >)
    message(STATUS "Code coverage targets: ${code_covered_binary_args}")

    add_custom_target(llvm_cov_show_html
            COMMAND llvm-cov-${LLVM_VERSION_MAJOR} show --format="html" --instr-profile combined_nondefault.profdata --ignore-filename-regex="build/.*" "${code_covered_binary_args}" > ${CMAKE_BINARY_DIR}/default_coverage.html
            COMMAND_EXPAND_LISTS
            BYPRODUCTS ${CMAKE_BINARY_DIR}/default_coverage.html
            DEPENDS merge_nondefault_profraw
    )
    add_custom_target(llvm_cov_report
            COMMAND llvm-cov-${LLVM_VERSION_MAJOR} report --instr-profile ${CMAKE_BINARY_DIR}/combined_nondefault.profdata --ignore-filename-regex="build/.*" "${code_covered_binary_args}" > ${CMAKE_BINARY_DIR}/default_coverage.report
            COMMAND_EXPAND_LISTS
            BYPRODUCTS ${CMAKE_BINARY_DIR}/default_coverage.report
            DEPENDS merge_nondefault_profraw
    )
    add_custom_target(llvm_cov_export_lcov
            COMMAND llvm-cov-${LLVM_VERSION_MAJOR} export --format="lcov" --instr-profile ${CMAKE_BINARY_DIR}/combined_nondefault.profdata --ignore-filename-regex="build/.*" "${code_covered_binary_args}" > ${CMAKE_BINARY_DIR}/default_coverage.lcov
            COMMAND_EXPAND_LISTS
            BYPRODUCTS ${CMAKE_BINARY_DIR}/default_coverage.lcov
            DEPENDS merge_nondefault_profraw
    )
endif (NES_COMPUTE_COVERAGE)

# This function registers a test with gtest_discover_tests
function(add_nes_test)
    add_executable(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-test-util)
    if (NES_ENABLE_PRECOMPILED_HEADERS)
        target_precompile_headers(${TARGET_NAME} REUSE_FROM nes-common)
        # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
        target_compile_options(${TARGET_NAME} PUBLIC "-fPIC")
    endif ()
    set(TEST_ENVVARS "")
    if (NES_COMPUTE_COVERAGE)
        set(TEST_ENVVARS PROPERTIES ENVIRONMENT "LLVM_PROFILE_FILE=./${TARGET_NAME}%p.profraw")
    endif ()
    gtest_discover_tests(${TARGET_NAME} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 30 ${TEST_ENVVARS})
    message(STATUS "Added ut ${TARGET_NAME}")
endfunction()

function(add_nes_common_test)
    add_nes_test(${ARGN})
endfunction()

function(add_compiler_unit_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-compiler)
endfunction()

function(add_nes_unit_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-data-types)
endfunction()

function(add_nes_integration_test)
    # create a test executable that may contain multiple source files.
    # first param is TARGET_NAME
    add_executable(${ARGN})
    set(TARGET_NAME ${ARGV0})
    add_dependencies(integration_tests ${TARGET_NAME})
    if (NES_ENABLE_PRECOMPILED_HEADERS)
        target_precompile_headers(${TARGET_NAME} REUSE_FROM nes-common)
    endif ()
    target_link_libraries(${TARGET_NAME} nes-coordinator-test-util)
    gtest_discover_tests(${TARGET_NAME} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR} DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 30)
    message(STATUS "Added it test ${TARGET_NAME}")
endfunction()
