# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# In this function we create target for system tests. We make heavy use of cache files to speed up the tests when
# running them multiple times. The cache files are created by the nebuli tool. The cache files are created in the
# build directory in the cache folder. After the initial creation of the cache files the are only updated if the
# system test file changes.

add_subdirectory(utils)