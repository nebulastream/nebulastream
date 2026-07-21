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
#include <optional>
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
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{
/// There is no predetermined tuple size for output-formatted tuples, we pass 0 as placeholder
OutputFormatterBufferRef::OutputFormatterBufferRef(
    std::vector<Field> fields, std::shared_ptr<OutputFormatter> formatter, const uint64_t bufferSize)
    : TupleBufferRef(bufferSize, bufferSize, 0), fields(std::move(fields)), formatter(std::move(formatter))
{
}

Record
OutputFormatterBufferRef::readRecord(const std::vector<Record::RecordFieldIdentifier>&, const RecordBuffer&, nautilus::val<uint64_t>&) const
{
    INVARIANT(false, "OutputFormatterBufferRef should not be able to read records.");
    std::unreachable();
}

TupleBufferRef::WriteRecordResult OutputFormatterBufferRef::writeRecord(
    nautilus::val<uint64_t>& bytesWritten,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<bool> successful{true};
    /// This will be incremented by the amount of bytes written for each field to calculate the field address
    nautilus::val<uint64_t> writtenForThisRecord{0};
    if (bytesWritten >= bufferSize)
    {
        successful = false;
    }
    else
    {
        const auto bufferAddress = recordBuffer.getMemArea();
        const auto recordAddress = bufferAddress + bytesWritten;

        /// Format the record, grouping maximal contiguous RUNS of non-nullable lazy passthrough fields (raw
        /// input bytes) so the formatter can emit each run as a single coalesced copy (e.g. CSV) instead of
        /// one copy per field. Which fields are lazy passthrough is fixed at trace time, so the grouping is
        /// pure host-side control flow computed up front; the emit loop then uses a single static induction
        /// variable (as elsewhere in this file), emitting a coalesceable run at its start, skipping the run's
        /// interior fields, and writing everything else per-field. Computed/nullable fields (and any run the
        /// formatter declines, e.g. non-CSV or a multi-byte delimiter) go through per-field writeFormattedValue().
        const std::size_t numberOfFields = fields.size();

        /// Host pre-pass: is each field a non-nullable lazy passthrough value (raw input bytes)? Also track
        /// whether EVERY field is non-nullable (lazy or computed) -- the broader gate for the whole-record
        /// fast path, which now handles computed numeric values too.
        std::vector<bool> isLazyPassthrough(numberOfFields);
        bool allNonNullable = numberOfFields > 0;
        for (std::size_t i = 0; i < numberOfFields; ++i)
        {
            const auto& value = rec.read(fields.at(i).name);
            isLazyPassthrough[i] = value.isLazyValue() and not value.isNullable();
            allNonNullable = allNonNullable and not value.isNullable();
        }

        /// Whole-record fast path: when EVERY field is non-nullable, offer the record to the formatter's
        /// tryWriteRecordAllPassthrough(). It hoists a SINGLE per-record fit-check (a worst-case reservation)
        /// and, on the common fits case, emits the record with no per-field guard -- lazy values via a
        /// bounded fixed-width store, computed values via a direct serialize with the parser's fit-check
        /// folded away. The formatter may still decline host-side (e.g. JSON only handles NUMERIC fields);
        /// it returns nullopt then and we drop to per-field.
        bool recordEmitted = false;
        if (allNonNullable)
        {
            {
                std::vector<DataType> fieldTypes;
                fieldTypes.reserve(numberOfFields);
                for (const auto& field : fields)
                {
                    fieldTypes.push_back(field.type);
                }
                const nautilus::val<uint64_t> remainingMainBytes{bufferSize - bytesWritten};
                if (const auto recordWritten = formatter->tryWriteRecordAllPassthrough(
                        rec, fieldTypes, recordAddress, remainingMainBytes, recordBuffer, bufferProvider))
                {
                    writtenForThisRecord += *recordWritten;
                    recordEmitted = true;
                }
            }
        }

        /// Host: length of the coalesceable run STARTING at each field (>= 2 => coalesce), and whether a field
        /// is interior to a run started earlier (=> skip, already emitted by that run's start). Only formed
        /// when the formatter guarantees it accepts such runs (supportsRunCoalescing); otherwise every field
        /// stays per-field so a declined run can never drop its interior fields.
        std::vector<std::size_t> runLengthAt(numberOfFields, 0);
        std::vector<bool> interiorToRun(numberOfFields, false);
        for (std::size_t i = 0; not recordEmitted and formatter->supportsRunCoalescing() and i < numberOfFields;)
        {
            if (not isLazyPassthrough[i])
            {
                ++i;
                continue;
            }
            std::size_t runEnd = i;
            while (runEnd < numberOfFields and isLazyPassthrough[runEnd])
            {
                ++runEnd;
            }
            if (runEnd - i >= 2)
            {
                runLengthAt[i] = runEnd - i;
                for (std::size_t k = i + 1; k < runEnd; ++k)
                {
                    interiorToRun[k] = true;
                }
            }
            i = runEnd;
        }

        for (nautilus::static_val<uint64_t> i = 0; not recordEmitted and i < numberOfFields; ++i)
        {
            const std::size_t fieldIndex = i;
            if (interiorToRun.at(fieldIndex))
            {
                continue;
            }
            const auto fieldAddress = recordAddress + writtenForThisRecord;
            const nautilus::val<uint64_t> remainingBytes{bufferSize - bytesWritten - writtenForThisRecord};

            std::optional<nautilus::val<uint64_t>> coalesced;
            if (const std::size_t runLength = runLengthAt.at(fieldIndex); runLength >= 2)
            {
                std::vector<Record::RecordFieldIdentifier> runFieldNames;
                runFieldNames.reserve(runLength);
                for (std::size_t k = fieldIndex; k < fieldIndex + runLength; ++k)
                {
                    runFieldNames.push_back(fields.at(k).name);
                }
                const bool runEndsRecord = fieldIndex + runLength == numberOfFields;
                coalesced = formatter->tryWriteCoalescedRun(
                    rec, runFieldNames, runEndsRecord, fieldAddress, remainingBytes, recordBuffer, bufferProvider);
            }
            if (coalesced)
            {
                writtenForThisRecord += *coalesced;
            }
            else
            {
                const auto& [name, type] = fields.at(fieldIndex);
                writtenForThisRecord
                    += formatter->writeFormattedValue(rec.read(name), type, i, fieldAddress, remainingBytes, recordBuffer, bufferProvider);
            }
        }
    }
    return {.successful = successful, .writtenRecords = writtenForThisRecord};
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
