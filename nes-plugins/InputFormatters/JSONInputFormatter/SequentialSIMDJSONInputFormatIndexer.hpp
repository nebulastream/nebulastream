/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <ostream>
#include <string>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <SIMDJSONFIF.hpp>

namespace NES
{

class SequentialSIMDJSONInputFormatIndexer final : public InputFormatIndexer<SequentialSIMDJSONInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialJSON";
    static constexpr bool IsSequential = true;

    using IndexerMetaData = SIMDJSONMetaData;
    using FieldIndexFunctionType = SIMDJSONFIF;
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';

    SequentialSIMDJSONInputFormatIndexer() = default;
    ~SequentialSIMDJSONInputFormatIndexer() = default;

    static void indexRawBuffer(SIMDJSONFIF& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const SIMDJSONMetaData& metaData);
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialSIMDJSONInputFormatIndexer&);
};

}
