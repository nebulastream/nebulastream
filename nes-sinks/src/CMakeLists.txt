# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(SinksParsing)

add_source_files(nes-sinks-validation
        SinkDescriptor.cpp
        SinkCatalog.cpp
)

add_source_files(nes-sinks
        Sink.cpp
        SinkProvider.cpp
)

# Register plugins — runtime side (Sink factory)
add_plugin(File Sink FileSink.cpp)
add_plugin(Print Sink PrintSink.cpp)
add_plugin(Network Sink NetworkSink.cpp)
add_plugin(Tokio Sink TokioSink.cpp)

# Register plugins — validation side. Compile the dedicated validation TUs
# into nes-sinks-validation so its registrar resolves Register*SinkValidation
# without pulling runtime symbols.
add_plugin(File SinkValidation FileSinkValidation.cpp)
add_plugin(Print SinkValidation PrintSinkValidation.cpp)
add_plugin(Network SinkValidation NetworkSinkValidation.cpp)
add_plugin(Tokio SinkValidation TokioSinkValidation.cpp)
