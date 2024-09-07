# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### Row Layout Tests ###
add_nes_runtime_test(row-memory-layout-tests "RowMemoryLayoutTest.cpp")

### Row Layout Tests ###
add_nes_runtime_test(columnar-memory-layout-tests "ColumnarMemoryLayoutTest.cpp")

### Dynamic Memory Layout Tests ###
add_nes_runtime_test(dynamic-memory-layout-tests "DynamicMemoryLayoutTest.cpp")
