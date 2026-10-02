# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# GObject calls its class and type callbacks through generic function pointer types by design, which
# -fsanitize=function reports on every type registration. Keep the other UndefinedBehaviorSanitizer checks.
string(APPEND VCPKG_C_FLAGS " -fno-sanitize=function")
string(APPEND VCPKG_CXX_FLAGS " -fno-sanitize=function")
