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

#include <cstddef>
#include <ostream>
#include <string_view>

#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <InputFormatIndexerRegistry.hpp>

namespace NES::InputFormatters
{

class CSVInputFormatIndexer final : public InputFormatIndexer
{
public:
    explicit CSVInputFormatIndexer(InputFormatIndexerRegistryArguments args);
    ~CSVInputFormatIndexer() override = default;

    CSVInputFormatIndexer(const CSVInputFormatIndexer&) = delete;
    CSVInputFormatIndexer& operator=(const CSVInputFormatIndexer&) = delete;
    CSVInputFormatIndexer(CSVInputFormatIndexer&&) = delete;
    CSVInputFormatIndexer& operator=(CSVInputFormatIndexer&&) = delete;

    void indexTuple(std::string_view tuple, FieldOffsetsType* fieldOffsets, FieldOffsetsType startIdxOfTuple) const override;

    FirstAndLastTupleDelimiterOffsets indexBuffer(std::string_view bufferView, FieldOffsets& fieldOffsets) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    ParserConfig config;
    size_t numberOfFieldsInSchema;
};

}
