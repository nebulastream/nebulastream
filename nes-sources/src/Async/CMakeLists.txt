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
    AsyncSourceHandle.cpp
    AsyncSourceRunner.cpp
    IOThread.cpp
    Sources/AsyncSource.cpp
    Sources/IoUringAsyncSource.cpp
)

# Register plugins
add_plugin(File Source nes-sources Sources/FileSource.cpp)
add_plugin(File SourceValidation nes-sources Sources/FileSource.cpp)
add_plugin(TCP Source nes-sources Sources/TCPSource.cpp)
add_plugin(TCP SourceValidation nes-sources Sources/TCPSource.cpp)
# Async io_uring sources: deep-QD cold file reads + multishot-recv TCP (see IoUringAsyncSource.hpp)
add_plugin(IoUringFileAsync Source nes-sources Sources/IoUringAsyncFileSource.cpp)
add_plugin(IoUringFileAsync SourceValidation nes-sources Sources/IoUringAsyncFileSource.cpp)
add_plugin(IoUringTCP Source nes-sources Sources/IoUringAsyncTCPSource.cpp)
add_plugin(IoUringTCP SourceValidation nes-sources Sources/IoUringAsyncTCPSource.cpp)

# Systest adaptors
add_plugin(File InlineData nes-sources Sources/FileSource.cpp)
add_plugin(File FileData nes-sources Sources/FileSource.cpp)
add_plugin(TCP InlineData nes-sources Sources/TCPSource.cpp)
add_plugin(TCP FileData nes-sources Sources/TCPSource.cpp)
add_plugin(IoUringFileAsync InlineData nes-sources Sources/IoUringAsyncFileSource.cpp)
add_plugin(IoUringFileAsync FileData nes-sources Sources/IoUringAsyncFileSource.cpp)
add_plugin(IoUringTCP InlineData nes-sources Sources/IoUringAsyncTCPSource.cpp)
add_plugin(IoUringTCP FileData nes-sources Sources/IoUringAsyncTCPSource.cpp)
