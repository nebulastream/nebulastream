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
#include <Nautilus/Interface/BufferRef/OutputFormatterBufferRef.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/val_ptr.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{
OutputFormatterBufferRef::OutputFormatterBufferRef(
    std::vector<Field> fields, std::shared_ptr<OutputFormatter> formatter, const uint64_t tupleSize, const uint64_t bufferSize)
    : TupleBufferRef(bufferSize / tupleSize, bufferSize, tupleSize), fields(std::move(fields)), formatter(std::move(formatter))
{
}

Record
OutputFormatterBufferRef::readRecord(const std::vector<Record::RecordFieldIdentifier>&, const RecordBuffer&, nautilus::val<uint64_t>&) const
{
    INVARIANT(false, "OutputFormatterBufferRef should not be able to read records.");
    std::unreachable();
}

nautilus::val<size_t> OutputFormatterBufferRef::writeRecord(
    nautilus::val<uint64_t>& bytesWritten,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<bool> checkSpaceBeforeWriting) const
{
    nautilus::val isFirstRecordOfBuffer(bytesWritten == nautilus::val<uint64_t>(0));
    /// If there is no space left in this buffer, we emit
    if (checkSpaceBeforeWriting && (bytesWritten >= bufferSize))
    {
        return std::string::npos;
    }
    const auto bufferAddress = recordBuffer.getMemArea();
    /// Calculating the field offset is therefore straight forward
    auto recordAddress = bufferAddress + bytesWritten;
    /// This will be incremented by the amount of bytes written for each field to calculate the field address
    nautilus::val<size_t> writtenForThisRecord(0);
    /// This value counts the amount of actual fields of the sink schema that we visited already.
    /// Iterate through the vals of the record and pass them to the output formatter for formatting
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, fieldOffset] = fields.at(i);
        const auto fieldAddress = recordAddress + writtenForThisRecord;
        nautilus::val<unsigned long> remainingBytes(0);
        /// In case that we already continued writing into child buffers, because a single record did not fit into the buffer,
        /// the written bytes might be higher than the buffer size, leading to unwanted behaviour if subtracted
        if (!isFirstRecordOfBuffer || bytesWritten + writtenForThisRecord <= bufferSize)
        {
            remainingBytes = bufferSize - bytesWritten - writtenForThisRecord;
        }
        const auto& value = rec.read(name);
        const auto amountWritten = formatter->getFormattedValue(
            value, name, type, i, fieldAddress, remainingBytes, isFirstRecordOfBuffer, recordBuffer, bufferProvider);
        /// This signalizes that the buffer did not have enough remaining space to capture the formatted value.
        /// Therefore, we signalize the emit operator to emit the current buffer
        if (amountWritten == nautilus::val<size_t>(std::string::npos))
        {
            return std::string::npos;
        }
        writtenForThisRecord += amountWritten;
    }
    return writtenForThisRecord;
}

std::vector<Record::RecordFieldIdentifier> OutputFormatterBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform([](const Field& field) { return field.name; }) | std::ranges::to<std::vector>();
}

std::vector<DataType> OutputFormatterBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform([](const Field& field) { return field.type; }) | std::ranges::to<std::vector>();
}

}
