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
        ${PROJECT_SOURCE_DIR}/nes-input-formatters/src/InputFormatterDescriptor.cpp
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
add_plugin(File Source FileSource.cpp)
add_plugin(File InlineData FileSource.cpp)
add_plugin(File FileData FileSource.cpp)
add_plugin(Network Source NetworkSource.cpp)

# Register plugins — validation side. These TUs hold the Register*SourceValidation
# function and must compile into nes-sources-validation so the validation library's
# registrar can resolve them without pulling in any runtime symbols.
add_plugin(File SourceValidation FileSourceValidation.cpp)
add_plugin(Network SourceValidation NetworkSourceValidation.cpp)
add_plugin(Tokio SourceValidation TokioSourceValidation.cpp)

# TokioSource C++ class (runtime). Tokio-backed sources register their
# SourceValidation in TokioSourceValidation.cpp above.
add_source_files(nes-sources TokioSource.cpp)
