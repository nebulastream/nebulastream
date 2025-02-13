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

#include <cstdint>
#include <ostream>
#include <string_view>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>

namespace NES::InputFormatters
{

class CSVInputFormatter final : public InputFormatter
{
public:
    CSVInputFormatter() = default;
    ~CSVInputFormatter() override = default;

    CSVInputFormatter(const CSVInputFormatter&) = delete;
    CSVInputFormatter& operator=(const CSVInputFormatter&) = delete;
    CSVInputFormatter(CSVInputFormatter&&) = delete;
    CSVInputFormatter& operator=(CSVInputFormatter&&) = delete;

    void indexSpanningTuple(
        std::string_view tuple,
        std::string_view fieldDelimiter,
        FieldOffsetsType* fieldOffsets,
        FieldOffsetsType startIdxOfCurrentTuple,
        FieldOffsetsType endIdxOfCurrentTuple,
        FieldOffsetsType currentFieldIndex) override;

    BufferOffsets indexRawBuffer(
        std::string_view bufferView,
        FieldOffsets& fieldOffsets,
        std::string_view tupleDelimiter,
        std::string_view fieldDelimiter) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
};

}
