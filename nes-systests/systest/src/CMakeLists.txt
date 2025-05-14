# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-systest-lib
        SystestParser.cpp
        SystestRunner.cpp
        SystestGrpc.cpp
        SystestState.cpp
        SystestResultCheck.cpp
        SystestConfiguration.cpp
)

add_plugin(File InlineData nes-systest-lib InlineData.cpp)
add_plugin(File FileData nes-systest-lib FileData.cpp)

add_plugin(TCP InlineData nes-systest-lib InlineData.cpp)
#add_plugin(TCP FileData nes-systest-lib FileData.cpp)
