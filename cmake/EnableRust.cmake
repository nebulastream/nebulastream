# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)
FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.5 # Optionally specify a commit hash, version tag or branch here
)

FetchContent_MakeAvailable(Corrosion)
# Import targets defined in a package or workspace manifest `Cargo.toml` file
corrosion_import_crate(
        MANIFEST_PATH webulastream/Cargo.toml
        CRATES bridge_of_bridges
        IMPORTED_CRATES CRATES
        CRATE_TYPES staticlib
)

# Initialize empty lists
set(CXXFLAGS_LIST "")
set(RUSTFLAGS_LIST "")

# Add flags based on sanitizer options
# Unfortunately, compiling rust with sanitizers requires the nightly compiler, which we don't want to enable in general.
# If you are using the nightly compiler you can uncomment the additional rust flags
if (NES_ENABLE_THREAD_SANITIZER)
    #    list(APPEND RUSTFLAGS_LIST "-Zsanitizer=thread")
    list(APPEND CXXFLAGS_LIST "-fsanitize=thread")
elseif (NES_ENABLE_UB_SANITIZER)
    list(APPEND CXXFLAGS_LIST "-fsanitize=thread")
elseif (NES_ENABLE_ADDRESS_SANITIZER)
    #    list(APPEND RUSTFLAGS_LIST "-Zsanitizer=address")
    list(APPEND CXXFLAGS_LIST "-fsanitize=address")
endif ()

# Add libc++ if needed
if (${USING_LIBCXX})
    list(APPEND CXXFLAGS_LIST "-stdlib=libc++")
endif ()

# Join the lists with spaces to create the flag strings
list(JOIN CXXFLAGS_LIST " " ADDITIONAL_CXXFLAGS)
list(JOIN RUSTFLAGS_LIST " " ADDITIONAL_RUSTFLAGS)

# Create separate environment variables
set(ENV_VARS_LIST "")

if (NOT "${ADDITIONAL_CXXFLAGS}" STREQUAL "")
    # Use a single environment variable
    list(APPEND ENV_VARS_LIST CXXFLAGS=${ADDITIONAL_CXXFLAGS})
endif ()

if (NOT "${ADDITIONAL_RUSTFLAGS}" STREQUAL "")
    list(APPEND ENV_VARS_LIST RUSTFLAGS=${ADDITIONAL_RUSTFLAGS})
endif ()

# Set the property with semicolon-separated list
if (NOT "${ENV_VARS_LIST}" STREQUAL "")
    list(JOIN ENV_VARS_LIST " " ENV_VARS)
    message(STATUS "Additional Corrosion Environment Variables: ${ENV_VARS}")
    # WIP: Currently ENV_VARS is expanded into garbage if any environment contains multiple flags.
    set_target_properties(bridge_of_bridges
            PROPERTIES
            CORROSION_ENVIRONMENT_VARIABLES ${ENV_VARS})
endif ()
