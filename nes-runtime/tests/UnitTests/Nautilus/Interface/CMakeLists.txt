
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_runtime_test(nautilus-data-type-test "DataTypeTest.cpp")
add_nes_runtime_test(nautilus-custom-data-type-test "CustomDataTypeTest.cpp")
add_nes_runtime_test(nautilus-list-type-test "ListTypeTest.cpp")
add_nes_runtime_test(nautilus-text-type-test "TextTypeTest.cpp")

add_subdirectory(Hash)
add_subdirectory(HashMap)
add_subdirectory(PagedVector)
add_subdirectory(Fixed2DArray)
add_subdirectory(FixedPage)
