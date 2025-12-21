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

class SIMDJSONInputFormatIndexer final : public InputFormatIndexer<SIMDJSONInputFormatIndexer>
{
public:
    using IndexerMetaData = SIMDJSONMetaData;
    using FieldIndexFunctionType = SIMDJSONFIF;
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char FIELD_DELIMITER = ',';
    static constexpr char KEY_QUOTE = '"';

    SIMDJSONInputFormatIndexer() = default;
    ~SIMDJSONInputFormatIndexer() = default;

    static void indexRawBuffer(SIMDJSONFIF& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const SIMDJSONMetaData&);

    friend std::ostream& operator<<(std::ostream& os, const SIMDJSONInputFormatIndexer& sonInputFormatIndexer);
};

struct ConfigParametersSIMDJSON
{
    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap();
};
}
