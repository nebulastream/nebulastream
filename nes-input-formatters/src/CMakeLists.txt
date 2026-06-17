# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(InputParsers)
add_source_files(nes-input-formatters
        SequenceShredder.cpp
        SpanningTupleBuffer.cpp
        SpanningTupleBufferState.cpp
        LockingSequenceShredder.cpp
        LockingSpanningTupleBuffer.cpp
        PlainSpanningTupleBufferState.cpp
        RawValueParser.cpp
        InputFormatterTupleBufferRef.cpp
        InputFormatterDescriptor.cpp
        InputFormatterValidationProvider.cpp
)

# Register plugins
add_plugin(CSV InputFormatIndexer nes-input-formatters CSVInputFormatIndexer.cpp)
add_plugin(CSV InputFormatterValidation nes-input-formatters CSVInputFormatIndexer.cpp)
add_plugin(SequentialCSV InputFormatIndexer nes-input-formatters SequentialCSVInputFormatIndexer.cpp)
add_plugin(SequentialCSV InputFormatterValidation nes-input-formatters SequentialCSVInputFormatIndexer.cpp)
add_plugin(SIMDCSV InputFormatIndexer nes-input-formatters SIMDCSVInputFormatIndexer.cpp SIMDCSVKernel.cpp)
add_plugin(SIMDCSV InputFormatterValidation nes-input-formatters SIMDCSVInputFormatIndexer.cpp SIMDCSVKernel.cpp)
