# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# These crates link no C++ sources, so their tests run through cargo directly.
add_rust_cargo_test(model)
add_rust_cargo_test(migration)
add_rust_cargo_test(controller)
add_rust_cargo_test(coordinator)

# The coordinator simulation tests compile the coordinator crates against
# the deterministic madsim runtime, selected by the madsim cfg. They run
# after the per-crate Rust tests because all cargo invocations share one
# target directory.
if (NES_ENABLES_TESTS)
    add_test(
            NAME rust-coordinator-simulation
            COMMAND ${_CORROSION_CARGO} test -p coordinator --test simulation --locked --color=always
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    )
    get_property(_nes_rustc_for_sim CACHE Rust_COMPILER_CACHED PROPERTY VALUE)
    # RUN_SERIAL: each trial has a real-time watchdog around a virtual-time
    # simulation, so co-scheduled tests starving the CPU cause spurious
    # timeouts. Running alone matches the isolated runner the suite was
    # tuned on.
    set_tests_properties(rust-coordinator-simulation PROPERTIES
            ENVIRONMENT "RUSTUP_TOOLCHAIN=${Rust_TOOLCHAIN};RUSTC=${_nes_rustc_for_sim};PROTOC=$<TARGET_FILE:protobuf::protoc>;RUSTFLAGS=--cfg madsim"
            LABELS "rust"
            DEPENDS "rust-coordinator"
            RUN_SERIAL TRUE
    )
endif ()
