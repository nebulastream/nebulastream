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
#include <string_view>

#include <CSVInputFormatIndexer.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>

namespace NES
{

class SequentialCSVInputFormatIndexer : public InputFormatIndexer<SequentialCSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialCSV";
    static constexpr bool IsSequential = true;

    using IndexerMetaData = CSVMetaData;
    using FieldIndexFunctionType = FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>;

    explicit SequentialCSVInputFormatIndexer(const InputFormatterDescriptor& config);
    ~SequentialCSVInputFormatIndexer() = default;

    void indexRawBuffer(
        FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialCSVInputFormatIndexer& obj);

private:
    bool allowCommasInStrings{};
};

}
