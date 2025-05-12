# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-sources
        SourceThread.cpp
        SourceDescriptor.cpp
        Source.cpp
        SourceHandle.cpp
        SourceProvider.cpp
        SourceValidationProvider.cpp
)

# Register plugins
add_plugin(File Source nes-sources FileSource.cpp)
add_plugin(Memory Source nes-sources MemorySource.cpp)
add_plugin(File SourceValidation nes-sources FileSource.cpp)
add_plugin(Memory SourceValidation nes-sources MemorySource.cpp)
