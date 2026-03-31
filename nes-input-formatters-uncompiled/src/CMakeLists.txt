# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_source_files(nes-input-formatters-uncompiled
        UncompiledInputFormatterProvider.cpp
        UncompiledSequenceShredder.cpp
        UncompiledSTBuffer.cpp
        UncompiledSTBufferState.cpp
        UncompiledRawValueParser.cpp
        UncompiledInputFormatterTaskPipeline.cpp
        UncompiledSIMDJSONFIF.cpp
)

# Register plugins
add_plugin(UncompiledCSV UncompiledInputFormatIndexer nes-input-formatters-uncompiled UncompiledCSVInputFormatIndexer.cpp)
add_plugin(UncompiledCSV InputFormatterValidation nes-input-formatters-uncompiled UncompiledCSVInputFormatIndexer.cpp)
add_plugin(UncompiledJSON UncompiledInputFormatIndexer nes-input-formatters-uncompiled UncompiledSIMDJSONInputFormatIndexer.cpp)
add_plugin(UncompiledJSON InputFormatterValidation nes-input-formatters-uncompiled UncompiledSIMDJSONInputFormatIndexer.cpp)
