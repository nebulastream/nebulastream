# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# specific settings for Host dependencies (tools used at build time)
# (c.f. https://learn.microsoft.com/en-us/vcpkg/users/host-dependencies)
#
# We have a separate triplet for host dependencies as we do not want to build them
# with e.g. sanitizers but generally "as simple as possible".

# only build release as we assume that we don't have to debug e.g. protoc.
# Thus we save the time and space required for the debug build.
set(VCPKG_BUILD_TYPE release)
