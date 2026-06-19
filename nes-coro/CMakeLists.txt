# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# nes-coro: small, header-only support library for bridging Rust async (Tokio)
# into coroutine-driven C++. It owns the shared coroutine primitives used by the
# Rust FFI layers (buffer bindings, source/sink runtimes): a fire-and-forget
# DetachedTask driver and an AsyncSemaphore. It also re-exports libcoro so that
# consumers get `coro::task<T>` without each having to locate the package.

find_package(PkgConfig REQUIRED)
pkg_check_modules(libcoro REQUIRED IMPORTED_TARGET GLOBAL libcoro)

add_library(nes-coro INTERFACE)
target_include_directories(nes-coro
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)
target_link_libraries(nes-coro INTERFACE PkgConfig::libcoro)
