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
        Async/AsyncSourceRunner.cpp
        Async/IOThread.cpp
        Async/AsyncSourceHandle.cpp
        Async/Sources/AsyncSource.cpp
        Async/Sources/FileSource.cpp
        Async/Sources/TCPSource.cpp
        Blocking/BlockingSource.cpp
        Blocking/BlockingSourceHandle.cpp
        Blocking/BlockingSourceRunner.cpp
        Blocking/Sources/BlockingTCPSource.cpp
        SourceDescriptor.cpp
        SourceProvider.cpp
        SourceDataProvider.cpp
        SourceValidationProvider.cpp
        LogicalSource.cpp
        SourceCatalog.cpp
)

# Register plugins
add_plugin(File Source nes-sources Async/Sources/FileSource.cpp)
add_plugin(File SourceValidation nes-sources Async/Sources/FileSource.cpp)
add_plugin(BlockingFile Source nes-sources Blocking/Sources/BlockingFileSource.cpp)
add_plugin(BlockingFile SourceValidation nes-sources Blocking/Sources/BlockingFileSource.cpp)
add_plugin(TCP Source nes-sources Async/Sources/TCPSource.cpp)
add_plugin(TCP SourceValidation nes-sources Async/Sources/TCPSource.cpp)
add_plugin(BlockingTCP Source nes-sources Blocking/Sources/BlockingTCPSource.cpp)
add_plugin(BlockingTCP SourceValidation nes-sources Blocking/Sources/BlockingTCPSource.cpp)

# Systest adaptors
add_plugin(File InlineData nes-sources Async/Sources/FileSource.cpp)
add_plugin(File FileData nes-sources Async/Sources/FileSource.cpp)
add_plugin(BlockingFile InlineData nes-sources Blocking/Sources/BlockingFileSource.cpp)
add_plugin(BlockingFile FileData nes-sources Blocking/Sources/BlockingFileSource.cpp)
add_plugin(TCP InlineData nes-sources-registry Async/Sources/TCPSource.cpp)
add_plugin(TCP FileData nes-sources-registry Async/Sources/TCPSource.cpp)
add_plugin(BlockingTCP InlineData nes-sources-registry Blocking/Sources/BlockingTCPSource.cpp)
add_plugin(BlockingTCP FileData nes-sources-registry Blocking/Sources/BlockingTCPSource.cpp)
