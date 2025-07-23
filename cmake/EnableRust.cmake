# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Unfortunately, compiling rust with sanitizers requires the nightly compiler.
SET(Rust_RESOLVE_RUSTUP_TOOLCHAINS OFF)
SET(Rust_TOOLCHAIN "nightly")

set(CXXFLAGS_LIST "-std=c++23")
set(RUSTFLAGS_LIST "")
set(CARGOFLAGS_LIST "")

# Add flags based on sanitizer options
if (SANITIZER_OPTION STREQUAL "address")
    list(APPEND RUSTFLAGS_LIST "-Zsanitizer=address")
    list(APPEND CARGOFLAGS_LIST "-Zbuild-std")
    list(APPEND CXXFLAGS_LIST "-fsanitize=address")
elseif (SANITIZER_OPTION STREQUAL "undefined")
    list(APPEND CARGOFLAGS_LIST "-Zbuild-std")
    list(APPEND CXXFLAGS_LIST "-fsanitize=undefined")
elseif (SANITIZER_OPTION STREQUAL "thread")
    list(APPEND RUSTFLAGS_LIST "-Zsanitizer=thread")
    list(APPEND CARGOFLAGS_LIST "-Zbuild-std")
    list(APPEND CXXFLAGS_LIST "-fsanitize=thread")
else ()
    SET(Rust_RESOLVE_RUSTUP_TOOLCHAINS ON)
    UNSET(Rust_TOOLCHAIN)
endif ()

# Add libc++ if needed
if (${USING_LIBCXX})
    list(APPEND CXXFLAGS_LIST "-stdlib=libc++")
endif ()

include(FetchContent)
FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.5.2
)

FetchContent_MakeAvailable(Corrosion)

list(JOIN CARGOFLAGS_LIST " " ADDITIONAL_CARGOFLAGS)
# Import targets defined in a package or workspace manifest `Cargo.toml` file
corrosion_import_crate(
        MANIFEST_PATH nes-network/Cargo.toml
        CRATES nes_rust_bindings
        IMPORTED_CRATES CRATES
        CRATE_TYPES staticlib
        FLAGS ${ADDITIONAL_CARGOFLAGS}
)

# Arguments are passed to cargo via an environment which we attach to the  nes_rust_bindings target and is loaded by corrosion
list(JOIN CXXFLAGS_LIST " " ADDITIONAL_CXXFLAGS)
list(JOIN RUSTFLAGS_LIST " " ADDITIONAL_RUSTFLAGS)
set(ENV_VARS_LIST "")

if (NOT "${ADDITIONAL_CXXFLAGS}" STREQUAL "")
    list(APPEND ENV_VARS_LIST CXXFLAGS=${ADDITIONAL_CXXFLAGS})
endif ()

if (NOT "${ADDITIONAL_RUSTFLAGS}" STREQUAL "")
    list(APPEND ENV_VARS_LIST RUSTFLAGS=${ADDITIONAL_RUSTFLAGS})
endif ()

# Set the property with semicolon-separated list
if (NOT "${ENV_VARS_LIST}" STREQUAL "")
    list(JOIN ENV_VARS_LIST " " ENV_VARS)
    message(STATUS "Additional Corrosion Environment Variables: ${ENV_VARS}")
    set_property(
            TARGET nes_rust_bindings
            PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS_LIST})
endif ()
