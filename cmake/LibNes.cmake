# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Umbrella shared library bundling all core components into a single libnes.so.
# Included by the top-level CMakeLists.txt after all component subdirectories are added.
#
# Rationale (dynamic plugin loading): a runtime-loaded plugin (.so) and its host executable must
# share one copy of every registry singleton and of all global state. Converting each component to
# its own shared library is not viable (the component graph contains PUBLIC link cycles, e.g.
# nes-logical-operators <-> nes-query-optimizer); instead the components stay STATIC and are
# whole-archived into this one shared library. Executables and plugin MODULEs link libnes and
# nothing else from the component set — linking a component statically next to libnes would
# duplicate registry state and re-run registration glue (duplicate-registration abort at startup).
#
# Requires CMake >= 3.27 for $<COMPILE_ONLY:...> (the repo's minimum is lower, but all supported
# build environments ship newer CMake).
#
# The components' INTERFACE WHOLE_ARCHIVE glue links (registration TUs) fire when libnes links
# the components, so all registries are populated inside the shared library. External static
# archives (vcpkg/nix, all built with -fPIC) and the components' PRIVATE deps are absorbed via
# normal as-needed linking.

set(LIBNES_COMPONENTS
        nes-grpc
        nes-common
        nes-configurations
        nes-data-types
        nes-memory
        nes-inference
        nes-sources
        nes-sinks
        nes-input-formatters
        nes-input-formatter-provider
        nes-output-formatters
        nes-nautilus
        nes-logical-operators
        nes-physical-operators
        nes-query-optimizer
        nes-query-compiler
        # nes-query-engine-interface is deliberately absent: it compiles QueryEngineConfiguration.cpp,
        # which is also part of nes-query-engine — whole-archiving both duplicates the symbols.
        nes-query-engine
        nes-runtime
        nes-executable
        nes-distributed
        nes-single-node-worker-interface
        nes-single-node-worker-lib
)

# No sources of its own: every object comes from the whole-archived component libraries.
add_library(libnes SHARED)
set_target_properties(libnes PROPERTIES LINKER_LANGUAGE CXX OUTPUT_NAME nes)

# Third-party libraries whose symbols are referenced from headers that consumers (executables,
# plugin MODULEs) compile inline — e.g. fmt via the logging/error macros, cpptrace via
# CPPTRACE_TRY, yaml-cpp via the config templates, grpc from server main functions. Consumers
# must link them directly since libnes's PRIVATE deps don't propagate. (${GRPC_LIBRARIES} is
# set by the root CMakeLists; its find_package(gRPC) runs at root scope, so the targets are
# visible here.)
find_package(fmt CONFIG REQUIRED)
find_package(cpptrace CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
target_link_libraries(libnes INTERFACE
        fmt::fmt cpptrace::cpptrace yaml-cpp::yaml-cpp spdlog::spdlog ${GRPC_LIBRARIES})

foreach (component IN LISTS LIBNES_COMPONENTS)
    # WHOLE_ARCHIVE pulls every object of the component archive into the shared library,
    # including registry singletons and otherwise-unreferenced code that plugins may need.
    target_link_libraries(libnes PRIVATE $<LINK_LIBRARY:WHOLE_ARCHIVE,${component}>)
    # Consumers compile against the components' headers but link only libnes itself.
    target_link_libraries(libnes INTERFACE $<COMPILE_ONLY:${component}>)
endforeach ()
