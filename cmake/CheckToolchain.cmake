# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Picks a standard c++ library. By default we opt into libc++ for its hardening mode. However if libc++ is not available
# we fallback to libstdc++. The user can manually opt out of libc++ by disabling the USE_LIBCXX_IF_AVAILABLE option.
# Currently NebulaStream requires Libc++-19 or Libstdc++-14 or above.

include(CheckCXXSourceCompiles)

set(USING_LIBSTDCXX ON)

add_compile_options(-stdlib=libc++)
# Currently C++20 threading features are hidden behind the feature flag
add_compile_options(-fexperimental-library)
# Enable Libc++ hardening mode
add_compile_definitions($<$<CONFIG:DEBUG>:_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_DEBUG>)
add_compile_definitions($<$<CONFIG:RelWithDebInfo>:_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST>)
