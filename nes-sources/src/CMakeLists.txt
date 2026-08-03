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
        SourceDataProvider.cpp
        SourceRegistry.cpp
        SourceValidationRegistry.cpp
        InlineDataRegistry.cpp
        FileDataRegistry.cpp
        SourceValidationProvider.cpp
        LogicalSource.cpp
        SourceCatalog.cpp
        FileSource.cpp
        NetworkSource.cpp
)

add_registry_entry(Source File)
add_registry_entry(Source Network HEADER Sources/NetworkSource.hpp)
add_registry_entry(SourceValidation File)
add_registry_entry(SourceValidation Network HEADER Sources/NetworkSource.hpp)

# Systest adaptors
add_registry_entry(InlineData File)
add_registry_entry(FileData File)
