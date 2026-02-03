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
#include <OutputFormatters/OutputFormatter.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{

class CSVOutputFormatter : public OutputFormatter
{
public:
    explicit CSVOutputFormatter(const size_t numberOfFields, const bool escapeString);

    /// Write the string formatted VarVal value into the record buffer
    [[nodiscard]] nautilus::val<size_t> getFormattedValue(
        VarVal value,
        const Record::RecordFieldIdentifier& fieldName,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const nautilus::val<bool>& allowChildren,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    friend std::ostream& operator<<(std::ostream& out, const CSVOutputFormatter& format);

private:
    bool escapeStrings;
};

}

FMT_OSTREAM(NES::OutputFormatter);
