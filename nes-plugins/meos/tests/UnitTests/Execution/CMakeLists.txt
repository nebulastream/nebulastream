# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_nes_execution_test(meos-creation-test "Meos01.cpp")
target_link_libraries(meos-creation-test nes-meos)

add_nes_execution_test(meos-trajectoryfromCSV "Meos02.cpp")
target_link_libraries(meos-trajectoryfromCSV nes-meos)

# test with postgres, requires libpq.so and postgres developer libraries
# add_nes_execution_test(meos-postgres "Meos03.cpp")
# target_link_libraries(meos-postgres nes-meos)

