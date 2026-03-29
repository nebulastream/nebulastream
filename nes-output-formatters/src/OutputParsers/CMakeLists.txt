# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(DefaultINT8 OutputParser nes-output-formatters DefaultINT8OutputParser.cpp)
add_plugin(DefaultINT16 OutputParser nes-output-formatters DefaultINT16OutputParser.cpp)
add_plugin(DefaultINT32 OutputParser nes-output-formatters DefaultINT32OutputParser.cpp)
add_plugin(DefaultINT64 OutputParser nes-output-formatters DefaultINT64OutputParser.cpp)
add_plugin(DefaultUINT8 OutputParser nes-output-formatters DefaultUINT8OutputParser.cpp)
add_plugin(DefaultUINT16 OutputParser nes-output-formatters DefaultUINT16OutputParser.cpp)
add_plugin(DefaultUINT32 OutputParser nes-output-formatters DefaultUINT32OutputParser.cpp)
add_plugin(DefaultUINT64 OutputParser nes-output-formatters DefaultUINT64OutputParser.cpp)
add_plugin(DefaultF32 OutputParser nes-output-formatters DefaultF32OutputParser.cpp)
add_plugin(DefaultF64 OutputParser nes-output-formatters DefaultF64OutputParser.cpp)
add_plugin(DefaultBOOL OutputParser nes-output-formatters DefaultBOOLOutputParser.cpp)
add_plugin(DefaultCHAR OutputParser nes-output-formatters DefaultCHAROutputParser.cpp)
