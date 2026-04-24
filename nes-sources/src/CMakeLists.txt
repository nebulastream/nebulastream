# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-sources-validation
        SourceDescriptor.cpp
        LogicalSource.cpp
        SourceCatalog.cpp
        SourceValidationProvider.cpp
)

add_source_files(nes-sources
        SourceThread.cpp
        Source.cpp
        SourceThreadHandle.cpp
        SourceProvider.cpp
        SourceDataProvider.cpp
)

# Register plugins — runtime side (source class + systest adaptors)
add_plugin(File Source nes-sources FileSource.cpp)
add_plugin(File InlineData nes-sources FileSource.cpp)
add_plugin(File FileData nes-sources FileSource.cpp)

# Register plugins — validation side. These TUs hold the Register*SourceValidation
# function and must compile into nes-sources-validation so the validation library's
# registrar can resolve them without pulling in any runtime symbols.
add_plugin(File SourceValidation nes-sources-validation FileSourceValidation.cpp)
add_plugin(Tokio SourceValidation nes-sources-validation TokioSourceValidation.cpp)

# Tokio source C++ class (runtime side). Tokio-backed sources register their
# SourceValidation in TokioSourceValidation.cpp above; the runtime class below
# is instantiated by the runtime source provider for all tokio-backed plugins.
add_source_files(nes-sources TokioSource.cpp)
