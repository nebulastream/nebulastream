set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# boost-context does not recognize arm64
if (PORT STREQUAL "boost-context")
    SET(VCPKG_CMAKE_CONFIGURE_OPTIONS -DCMAKE_SYSTEM_PROCESSOR=aarch64)
endif ()
