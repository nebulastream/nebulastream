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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <ostream>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The output formatter is responsible for converting a Record into a string of a given Output Format like CSV or JSON
/// Output formatters are stateless, meaning that they must be able to produce valid output in a streaming fashion.
/// Output formatters are used by the OutputFormatterBufferRef during the last emit before a sink.
class OutputFormatter
{
public:
    explicit OutputFormatter(const size_t numberOfFields) : numberOfFields(numberOfFields) { }

    virtual ~OutputFormatter() noexcept = default;

    /// Format the field's contents into a string and write the string bytes into the field pointer address.
    /// If the formatted value does not fit into the record buffer, child buffers will be allocated to write the value into.
    /// The main buffer's space will always be utilized completely before writing in a child.
    /// Returns the number of bytes written into the record buffer (excluding the bytes written into the children).
    [[nodiscard]] virtual nautilus::val<uint64_t> writeFormattedValue(
        VarVal value,
        const Record::RecordFieldIdentifier& fieldName,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
        = 0;

    virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const OutputFormatter& obj);

protected:
    /// Number of fields in the output schema
    size_t numberOfFields;
};

}

template <std::derived_from<NES::OutputFormatter> OutputFormatter>
struct fmt::formatter<OutputFormatter> : fmt::ostream_formatter
{
};
