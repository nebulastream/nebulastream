set(RUST_PROJECT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust-lib")
set(RUST_BUILD_MODE "release")
set(RUST_LIB_NAME "librust_lib_cbg.a")
set(RUST_LIB_PATH "${RUST_PROJECT_DIR}/target/${RUST_BUILD_MODE}/${RUST_LIB_NAME}")
set(RUST_LIB_PATH_COMMON "${CMAKE_CURRENT_BINARY_DIR}/${RUST_LIB_NAME}")
set(RUST_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/include/RustCBG/RustCBG.hpp")
add_custom_command(
        OUTPUT ${RUST_LIB_PATH_COMMON} ${RUST_HEADER}
        # Build the rust code
        COMMAND ${CMAKE_COMMAND} -E env cargo build --${RUST_BUILD_MODE} --manifest-path ${RUST_PROJECT_DIR}/Cargo.toml
        # Copy the library, source- and header files to a common location
        COMMAND ${CMAKE_COMMAND} -E copy ${RUST_LIB_PATH} ${RUST_LIB_PATH_COMMON}
        COMMAND ${CMAKE_COMMAND} -E copy "${RUST_PROJECT_DIR}/include/RustCBG.hpp" ${RUST_HEADER}

        WORKING_DIRECTORY ${RUST_PROJECT_DIR}
        COMMENT "Building Rust project with Cargo"
        VERBATIM
)

add_custom_target(rust_build_cbg ALL
        DEPENDS ${RUST_LIB_PATH_COMMON} ${RUST_HEADER}
)
add_library(rust-lib-cbg STATIC IMPORTED GLOBAL)
set_target_properties(rust-lib-cbg PROPERTIES
        IMPORTED_LOCATION ${RUST_LIB_PATH_COMMON}
)
target_include_directories(rust-lib-cbg INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)
