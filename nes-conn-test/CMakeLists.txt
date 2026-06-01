# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Connectivity tests (conn tests)
# The whole body is gated on NES_ENABLES_TESTS so a production
# configuration (NES_ENABLES_TESTS=OFF) gets nothing from this dir.
if (NES_ENABLES_TESTS)
    # conntest-harness — the connectivity-test stepper. It drives a bare
    # Source / Sink via the registry directly (not the engine's SourceThread /
    # SinkProvider lowering), executing one command per line over the runner's
    # unix-domain control socket so the runner can sequence open/fill/close (or
    # start/write/stop) deterministically and SIGKILL+respawn it for reconnect.
    add_executable(conntest-harness
            driver/Binder.cpp
            driver/ConntestStarter.cpp
            driver/ControlChannel.cpp
            driver/Report.cpp
            driver/SessionCommand.cpp
            driver/SinkSession.cpp
            driver/SourceSession.cpp
    )

    # Driver-internal headers (ControlChannel.hpp) are included with angle
    # brackets per the repo's format rule (see
    # `.claude/skills/nes-format/SKILL.md` — quote-include rewrite). Expose
    # the driver/ directory so the search finds them.
    target_include_directories(conntest-harness PRIVATE driver)

    # OpenSSL libcrypto provides the streaming SHA-256 (EVP_*) primitives
    # the harness uses for the FILL/DRAIN reply's `sha` field. Already
    # pulled in transitively elsewhere in the build; declare it explicitly
    # so the link surface stays auditable.
    find_package(OpenSSL REQUIRED)

    # argparse backs the *Starter command-line parser (driver/ConntestStarter.cpp).
    # IMPORTED targets from find_package are directory-scoped, so call it here
    # rather than relying on transitive propagation.
    find_package(argparse CONFIG REQUIRED)

    # M2.1 source-mode link deps (conn-tests-m2.1-logistics.md §3.7).
    #
    # Deviations from the contract's literal target names, after grepping
    # add_library() in this checkout:
    #   - The contract lists `nes-catalogs`; no such target exists. The
    #     SourceCatalog implementation lives in nes-sources
    #     (nes-sources/src/SourceCatalog.cpp), so linking nes-sources
    #     already provides it.
    #   - The contract lists `nes-logger`; no such target exists. The
    #     NesLogger / spdlog facade lives in nes-common
    #     (nes-common/src/Util/Logger/), so the logger-quarantine TU is
    #     served by linking nes-common.
    #   - The contract lists `magic_enum::magic_enum` explicitly; we drop
    #     the explicit reference and rely on transitive PUBLIC propagation
    #     from nes-common (which calls find_package(magic_enum) and
    #     PUBLIC-links it at nes-common/CMakeLists.txt:64,73). IMPORTED
    #     targets defined by find_package are scoped to the directory the
    #     call was made in; a sibling subdirectory like nes-conn-test
    #     cannot reference `magic_enum::magic_enum` by name unless it
    #     either calls find_package itself or — as we do here — pulls
    #     it in via PUBLIC propagation from a target that did.
    # nes-sources-registry is the per-component plugin registry target
    # created by create_registries_for_component() in nes-sources/CMakeLists.txt
    # (see cmake/PluginRegistrationUtil.cmake L122-L128).
    target_link_libraries(conntest-harness PRIVATE
            nes-sql-parser            # GF-35 parser + binder (ManagedAntlrParser, StatementBinder)
            nes-sources               # GF-31 Source; also owns SourceCatalog
            nes-sources-registry      # GF-29 registry lookup
            nes-sinks                 # M2.2 Sink; SinkProvider::lower, SinkCatalog, BackpressureHandler
            nes-sinks-registry        # M2.2 sink registry lookup (SinkRegistry / SinkValidationRegistry)
            nes-memory                # GF-31 / INV-3 BufferManager
            nes-common                # GF-46 Logger + GF-39 magic_enum (transitively)
            OpenSSL::Crypto           # EVP_sha256 streaming digest (FILL/DRAIN reply sha)
            argparse::argparse        # *Starter command-line parser
    )

    # Force-link every active Source plugin even though the harness has
    # no compile-time reference to any of them: each plugin only enters
    # the binary via its registry's static-init Register* call, which
    # the linker strips under dead-code elimination if the archive isn't
    # whole-linked.
    #
    # The `Source_plugin_libraries` GLOBAL property is populated by
    # `add_plugin_as_library()` in `cmake/PluginRegistrationUtil.cmake`
    # — one entry per `activate_optional_plugin(...)` in
    # `nes-plugins/CMakeLists.txt`. Iterating it means adding a new
    # source connector requires zero edits here.
    #
    # The iteration is DEFER-scheduled to the project root: the file(GLOB)
    # in the top-level CMakeLists.txt processes nes-* subdirs in
    # alphabetical order, so nes-conn-test/ runs before nes-plugins/ and
    # the GLOBAL property is empty if we read it now. Same pattern as
    # `generate_plugin_registrars` in PluginRegistrationUtil.cmake.
    #
    # The §3.7 contract suggests $<LINK_LIBRARY:WHOLE_ARCHIVE,...>, but
    # that generator expression needs CMake >= 3.24 and the repo's
    # top-level cmake_minimum_required is 3.21.0, so we use the explicit
    # linker-flag form. The dev container is Linux + ld.bfd/lld so
    # --whole-archive is the right spelling.
    #
    # add_dependencies is belt-and-suspenders: $<TARGET_FILE:...> inside
    # a plain link-libraries call does NOT register an automatic
    # build-order edge under Ninja, so without it the harness can be
    # linked before the plugin archive has been (re)built.
    cmake_language(EVAL CODE "
        cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL cmake_language EVAL CODE [[
            get_property(_source_plugin_libs GLOBAL PROPERTY Source_plugin_libraries)
            foreach (_plugin_lib IN LISTS _source_plugin_libs)
                target_link_libraries(conntest-harness PRIVATE
                        \"-Wl,--whole-archive\"
                        \$<TARGET_FILE:\${_plugin_lib}>
                        \"-Wl,--no-whole-archive\"
                )
                add_dependencies(conntest-harness \${_plugin_lib})
            endforeach ()
        ]])
    ")

    # Same force-link treatment for every active Sink plugin (M2.2): each sink
    # connector enters the binary only via its SinkRegistry static-init
    # Register* call, stripped under dead-code elimination unless whole-linked.
    # `Sink_plugin_libraries` is populated by `add_plugin_as_library()` — one
    # entry per `activate_optional_plugin("Sinks/<Name>" ...)` — so a new sink
    # connector needs zero edits here. DEFER-scheduled to the project root for
    # the same reason as the Source block above (nes-plugins/ is processed
    # after nes-conn-test/, so the GLOBAL property is empty if read now).
    cmake_language(EVAL CODE "
        cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL cmake_language EVAL CODE [[
            get_property(_sink_plugin_libs GLOBAL PROPERTY Sink_plugin_libraries)
            foreach (_plugin_lib IN LISTS _sink_plugin_libs)
                target_link_libraries(conntest-harness PRIVATE
                        \"-Wl,--whole-archive\"
                        \$<TARGET_FILE:\${_plugin_lib}>
                        \"-Wl,--no-whole-archive\"
                )
                add_dependencies(conntest-harness \${_plugin_lib})
            endforeach ()
        ]])
    ")

    # Root CMakeLists.txt sets a global CMAKE_CXX_STANDARD (23), which a
    # plain add_executable inherits. Pin it per-target as well to make the
    # requirement explicit and robust against any toolchain-file override.
    set_target_properties(conntest-harness PROPERTIES
            CXX_STANDARD 23
            CXX_STANDARD_REQUIRED ON
    )

    # The runner expects the binary at exactly
    # <build-dir>/nes-conn-test/conntest-harness. The root CMakeLists.txt
    # sets no global CMAKE_RUNTIME_OUTPUT_DIRECTORY, so the CMake default
    # already places it in <build>/nes-conn-test/. Pin it explicitly anyway
    # so the contract holds regardless of any future global override.
    set_target_properties(conntest-harness PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )

    # The repo-wide -fdebug-prefix-map=container=host is applied in the
    # root CMakeLists.txt (gated on HOST_NEBULASTREAM_ROOT), so every TU
    # — driver/, nes-sources, plugins, etc. has host paths in
    # its debug info and CLion's gutter breakpoints match without any
    # <mappings> element in the Remote Debug config.
    #
    # The conn-test debug + smoke flows run pytest INSIDE the per-plugin
    # compose `runner` service (the unix-domain control socket stays
    # container-internal), driven by the `conntest` CLI
    # (`nes-conn-test/runner/conntest`). The CLion run configs live under
    # .idea/, which is gitignored at the repo root — generate them at
    # configure time instead of checking in XMLs (CLion 2026.1 also does
    # not expand $PROJECT_DIR$ inside CLion_Remote's symbolFile attribute,
    # so `conntest install-clion` bakes the absolute host path in).
    # NES_HOST_REPO_ROOT is HOST_NEBULASTREAM_ROOT, resolved once in the root
    # CMakeLists.txt (which also warns when it is unset or mismatched). When set,
    # generate the run configs; otherwise skip quietly — the root already warned
    # and the prefix-map was not applied.
    if (NES_HOST_REPO_ROOT)
        # install-clion reads HOST_NEBULASTREAM_ROOT; pass the resolved value
        # through explicitly so the generated symbolFile/launcher paths match.
        # Idempotent; exits fast when already up to date.
        set(_CONNTEST_CLI
                "${CMAKE_SOURCE_DIR}/nes-conn-test/runner/conntest")
        if (EXISTS "${_CONNTEST_CLI}")
            execute_process(
                    COMMAND ${CMAKE_COMMAND} -E env
                            "HOST_NEBULASTREAM_ROOT=${NES_HOST_REPO_ROOT}"
                            python3 "${_CONNTEST_CLI}" install-clion --quiet
                    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
                    OUTPUT_VARIABLE _CLION_SETUP_OUT
                    ERROR_VARIABLE _CLION_SETUP_ERR
                    RESULT_VARIABLE _CLION_SETUP_RC
            )
            if (NOT _CLION_SETUP_RC EQUAL 0)
                message(WARNING
                        "conntest: install-clion failed (rc=${_CLION_SETUP_RC}). "
                        "Run it manually after build:\n"
                        "  ./nes-conn-test/runner/conntest install-clion\n"
                        "stderr: ${_CLION_SETUP_ERR}")
            endif ()
        endif ()
    endif ()

    # Coverage opt-in. Instruments conntest-harness so the lcov uploaded
    # by code_coverage.yml includes the harness driver and (transitively)
    # the connector code it exercises via the pytest battery.
    if (CODE_COVERAGE)
        target_compile_options(conntest-harness PRIVATE
                -fprofile-instr-generate -fcoverage-mapping
        )
        target_link_options(conntest-harness PRIVATE
                -fprofile-instr-generate -fcoverage-mapping
        )

        # Hook the pytest runner into ccov-all-export so the conntest
        # binaries show up in the lcov uploaded by code_coverage.yml.
        # The coverage lane runs the full conformance battery against
        # the instrumented build — no marker filter; the only difference
        # from the sanitizer lanes is the build flavor.
        if (TARGET ccov-all-processing)
            add_custom_target(ccov-run-conntest-pytest
                    COMMAND python3
                            ${CMAKE_CURRENT_SOURCE_DIR}/runner/conntest coverage
                            --build-dir ${CMAKE_BINARY_DIR}
                            --ccov-dir ${CMAKE_COVERAGE_OUTPUT_DIRECTORY}
                            --harness $<TARGET_FILE:conntest-harness>
                    DEPENDS ccov-preprocessing
                            conntest-harness
                    JOB_POOL ccov_serial_pool
                    VERBATIM
            )
            add_dependencies(ccov-all-processing ccov-run-conntest-pytest)
        endif ()
    endif ()
endif ()
