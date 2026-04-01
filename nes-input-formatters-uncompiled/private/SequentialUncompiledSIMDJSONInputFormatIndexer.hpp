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
#include <string_view>
#include <unordered_map>

#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatIndexer.hpp>
#include <UncompiledSIMDJSONFIF.hpp>

namespace NES
{

class SequentialUncompiledSIMDJSONInputFormatIndexer final : public UncompiledInputFormatIndexer<SequentialUncompiledSIMDJSONInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialUncompiledJSON";
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    static constexpr bool IsSequential = true;

    using UncompiledIndexerMetaData = UncompiledSIMDJSONMetaData;
    using UncompiledFieldIndexFunctionType = UncompiledSIMDJSONFIF;
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';

    SequentialUncompiledSIMDJSONInputFormatIndexer() = default;
    ~SequentialUncompiledSIMDJSONInputFormatIndexer() = default;

    static void indexRawBuffer(UncompiledSIMDJSONFIF& fieldIndexFunction, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledSIMDJSONMetaData& metaData);
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialUncompiledSIMDJSONInputFormatIndexer&);
};

}
