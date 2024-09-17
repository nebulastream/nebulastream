# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Watermark/CMakeLists.txt
<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Watermark/CMakeLists.txt

add_source_files(nes-execution
        EventTimeWatermarkAssignment.cpp
        IngestionTimeWatermarkAssignment.cpp
        MultiOriginWatermarkProcessor.cpp
        TimeFunction.cpp
)
========
add_source_files(nes-execution
        RegistryFunctionExecutable.cpp
)
>>>>>>>> 8d070c4b32 (feat(Execution) Creating an executable function registry and porting existing functions to it):nes-execution/src/Execution/Functions/Registry/CMakeLists.txt
========
add_source_files(nes-sources
        Parser.cpp
        CSVParser.cpp
)
>>>>>>>> ba050f6ff9 (chore(Build) Fixed rebase errors):nes-sources/src/Parsers/CMakeLists.txt
