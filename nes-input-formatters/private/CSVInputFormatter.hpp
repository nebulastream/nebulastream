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

#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <FieldOffsets.hpp>

namespace NES::InputFormatters
{

class CSVInputFormatter final : public InputFormatter<FieldOffsets, /* IsNativeFormat */ false>
{
public:
    explicit CSVInputFormatter(Sources::ParserConfig config, size_t numberOfFieldsInSchema);
    ~CSVInputFormatter() override = default;

    CSVInputFormatter(const CSVInputFormatter&) = delete;
    CSVInputFormatter& operator=(const CSVInputFormatter&) = delete;
    CSVInputFormatter(CSVInputFormatter&&) = delete;
    CSVInputFormatter& operator=(CSVInputFormatter&&) = delete;

    void
    setupFieldAccessFunctionForBuffer(FieldOffsets& fieldOffsets, const RawTupleBuffer& rawBuffer, const TupleMetaData&) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    Sources::ParserConfig config;
    size_t numberOfFieldsInSchema;
};

}
