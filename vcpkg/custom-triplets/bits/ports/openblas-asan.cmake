# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Hand-written x86 kernels have tight register constraints. ASan adds enough
# register pressure for Clang to reject them. Keep OpenBLAS unsanitized;
# callers and the surrounding NES integration remain instrumented.
set(VCPKG_CXX_FLAGS "")
set(VCPKG_C_FLAGS "")
