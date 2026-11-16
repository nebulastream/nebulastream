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

add_source_files(nes-sinks
        SinkDescriptor.cpp
        Sink.cpp
        SinkProvider.cpp
        SinkCatalog.cpp
        SinkRegistry.cpp
        SinkConfigRegistry.cpp
        SinkConfigSchemaRegistry.cpp
        BackpressureHandler.cpp
        FileSink.cpp
        PrintSink.cpp
        NetworkSink.cpp
)

add_registry_entry(Sink FileSink HEADER Sinks/FileSink.hpp)
add_registry_entry(SinkConfigSchema FileSink HEADER Sinks/FileSink.hpp)
add_registry_entry(SinkConfig FileSink HEADER Sinks/FileSink.hpp)
add_registry_entry(Sink PrintSink HEADER Sinks/PrintSink.hpp)
add_registry_entry(SinkConfigSchema PrintSink HEADER Sinks/PrintSink.hpp)
add_registry_entry(SinkConfig PrintSink HEADER Sinks/PrintSink.hpp)
add_registry_entry(Sink NetworkSink HEADER Sinks/NetworkSink.hpp)
add_registry_entry(SinkConfigSchema NetworkSink HEADER Sinks/NetworkSink.hpp)
add_registry_entry(SinkConfig NetworkSink HEADER Sinks/NetworkSink.hpp)
