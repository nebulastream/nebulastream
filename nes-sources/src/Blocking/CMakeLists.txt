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
    BlockingSource.cpp
    BlockingSourceHandle.cpp
    BlockingSourceRunner.cpp
)

# Register plugins
add_plugin(BlockingTCP Source nes-sources Sources/BlockingTCPSource.cpp)
add_plugin(BlockingTCP SourceValidation nes-sources Sources/BlockingTCPSource.cpp)
add_plugin(BlockingFile Source nes-sources Sources/BlockingFileSource.cpp)
add_plugin(BlockingFile SourceValidation nes-sources Sources/BlockingFileSource.cpp)
add_plugin(InMemory Source nes-sources Sources/InMemoryFileSource.cpp)
add_plugin(InMemory SourceValidation nes-sources Sources/InMemoryFileSource.cpp)
add_plugin(MmapFile Source nes-sources Sources/MmapFileSource.cpp)
add_plugin(MmapFile SourceValidation nes-sources Sources/MmapFileSource.cpp)

# Systest adaptors
add_plugin(BlockingTCP InlineData nes-sources Sources/BlockingTCPSource.cpp)
add_plugin(BlockingTCP FileData nes-sources Sources/BlockingTCPSource.cpp)
add_plugin(BlockingFile InlineData nes-sources Sources/BlockingFileSource.cpp)
add_plugin(BlockingFile FileData nes-sources Sources/BlockingFileSource.cpp)
add_plugin(InMemory InlineData nes-sources Sources/InMemoryFileSource.cpp)
add_plugin(InMemory FileData nes-sources Sources/InMemoryFileSource.cpp)
add_plugin(MmapFile InlineData nes-sources Sources/MmapFileSource.cpp)
add_plugin(MmapFile FileData nes-sources Sources/MmapFileSource.cpp)
