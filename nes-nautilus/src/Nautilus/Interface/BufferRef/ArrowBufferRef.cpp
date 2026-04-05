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
#include <Nautilus/Interface/BufferRef/ArrowBufferRef.hpp>

#include <cstdint>
#include <cstring>
#include <ranges>
#include <span>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/select.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

ArrowBufferRef::ArrowBufferRef(
    std::vector<Field> fields, const uint32_t numVarSizedColumns, const uint64_t capacity, const uint64_t bufferSize, const uint64_t tupleSize)
    : TupleBufferRef(capacity, bufferSize, tupleSize), fields(std::move(fields)), numVarSizedColumns(numVarSizedColumns)
{
}

namespace
{
nautilus::val<int8_t*> calculateArrowFieldAddress(
    const nautilus::val<int8_t*>& bufferAddress,
    nautilus::val<uint64_t>& recordIndex,
    const uint32_t fieldSize,
    const uint64_t dataOffset)
{
    const auto fieldOffset = recordIndex * fieldSize + dataOffset;
    return bufferAddress + fieldOffset;
}

nautilus::val<int8_t*> bitmapByteAddress(
    const nautilus::val<int8_t*>& bufferAddress,
    const uint64_t bitmapOffset,
    const nautilus::val<uint64_t>& recordIndex)
{
    auto totalOffset = nautilus::val<uint64_t>{bitmapOffset} + recordIndex / nautilus::val<uint64_t>{8};
    return bufferAddress + totalOffset;
}

/// Sets or clears the Arrow validity bit for the given record in the column's bitmap region.
/// Arrow convention: 1 = valid (not null), 0 = null.
void writeValidityBit(
    const nautilus::val<int8_t*>& bufferAddress,
    const uint64_t bitmapOffset,
    const nautilus::val<uint64_t>& recordIndex,
    const nautilus::val<bool>& isNull)
{
    auto bitmapByteAddr = bitmapByteAddress(bufferAddress, bitmapOffset, recordIndex);
    auto currentByte = readValueFromMemRef<uint8_t>(bitmapByteAddr);
    auto bitPos = static_cast<nautilus::val<uint8_t>>(recordIndex % nautilus::val<uint64_t>{8});
    auto mask = nautilus::val<uint8_t>{1} << bitPos;
    auto withBitSet = static_cast<nautilus::val<uint8_t>>(currentByte | mask);
    auto invertedMask = static_cast<nautilus::val<uint8_t>>(mask ^ nautilus::val<uint8_t>{0xFF});
    auto withBitCleared = static_cast<nautilus::val<uint8_t>>(currentByte & invertedMask);
    auto newByte = nautilus::select(!isNull, withBitSet, withBitCleared);
    VarVal{newByte}.writeToMemory(bitmapByteAddr);
}

/// Reads the Arrow validity bit for the given record from the column's bitmap region.
/// Returns true if the value is null (bit == 0).
nautilus::val<bool> readValidityBitAsNull(
    const nautilus::val<int8_t*>& bufferAddress,
    const uint64_t bitmapOffset,
    const nautilus::val<uint64_t>& recordIndex)
{
    auto bitmapByteAddr = bitmapByteAddress(bufferAddress, bitmapOffset, recordIndex);
    auto currentByte = readValueFromMemRef<uint8_t>(bitmapByteAddr);
    auto bitPos = static_cast<nautilus::val<uint8_t>>(recordIndex % nautilus::val<uint64_t>{8});
    auto bit = static_cast<nautilus::val<uint8_t>>((currentByte >> bitPos) & nautilus::val<uint8_t>{1});
    return bit == nautilus::val<uint8_t>{0};
}
}

Record ArrowBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    Record record;
    const auto bufferAddress = recordBuffer.getMemArea();
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& field = fields.at(i);
        if (not includesField(projections, field.name))
        {
            continue;
        }

        nautilus::val<bool> isNull{false};
        if (field.type.nullable)
        {
            isNull = readValidityBitAsNull(bufferAddress, field.bitmapOffset, recordIndex);
        }

        if (field.type.type == DataType::Type::BOOLEAN)
        {
            /// BOOLEAN: data is bit-packed — read bit at recordIndex from the data region.
            auto boolByteAddr = bitmapByteAddress(bufferAddress, field.dataOffset, recordIndex);
            auto currentByte = readValueFromMemRef<uint8_t>(boolByteAddr);
            auto bitPos = static_cast<nautilus::val<uint8_t>>(recordIndex % nautilus::val<uint64_t>{8});
            auto bit = static_cast<nautilus::val<uint8_t>>((currentByte >> bitPos) & nautilus::val<uint8_t>{1});
            auto boolVal = bit != nautilus::val<uint8_t>{0};
            if (field.type.nullable)
            {
                record.write(field.name, VarVal{boolVal, true, isNull});
            }
            else
            {
                record.write(field.name, VarVal{boolVal});
            }
        }
        else if (field.type.type == DataType::Type::VARSIZED)
        {
            /// VARSIZED: read start/end offsets from the offsets array, then locate data across child buffer(s)
            auto offsetsAddr = calculateArrowFieldAddress(bufferAddress, recordIndex, field.dataTypeSize, field.dataOffset);
            auto startOffset = readValueFromMemRef<int32_t>(offsetsAddr);
            auto endOffset = readValueFromMemRef<int32_t>(offsetsAddr + nautilus::val<uint64_t>{sizeof(int32_t)});
            auto length = static_cast<nautilus::val<uint64_t>>(endOffset - startOffset);

            auto dataPtr = invoke(
                {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                +[](const TupleBuffer* tupleBuffer, const uint32_t childSlot, const uint32_t numVarCols,
                    const int32_t globalStart, const uint64_t strLength) -> const int8_t*
                {
                    /// Zero-length read (null or empty string) — no child buffer access needed
                    if (strLength == 0)
                    {
                        return nullptr;
                    }

                    /// Walk child buffers for this column to find the one containing globalStart
                    int32_t cumulativeOffset = 0;
                    const auto totalChildren = tupleBuffer->getNumberOfChildBuffers();
                    for (uint32_t idx = childSlot; idx < totalChildren; idx += numVarCols)
                    {
                        auto childBuf = tupleBuffer->loadChildBuffer(VariableSizedAccess::Index{idx});
                        const auto usedBytes = static_cast<int32_t>(childBuf.getNumberOfTuples());
                        if (globalStart < cumulativeOffset + usedBytes)
                        {
                            const auto localOffset = globalStart - cumulativeOffset;
                            return reinterpret_cast<const int8_t*>(childBuf.getAvailableMemoryArea<uint8_t>().data()) + localOffset;
                        }
                        cumulativeOffset += usedBytes;
                    }
                    INVARIANT(false, "Arrow VARSIZED read: global offset {} exceeds child buffer data {}", globalStart, cumulativeOffset);
                    return nullptr;
                },
                recordBuffer.getReference(),
                nautilus::val<uint32_t>{field.childBufferSlot},
                nautilus::val<uint32_t>{numVarSizedColumns},
                startOffset,
                length);

            record.write(field.name, VarVal{VariableSizedData(dataPtr, length), field.type.nullable, isNull});
        }
        else if (field.type.nullable)
        {
            auto fieldAddress = calculateArrowFieldAddress(bufferAddress, recordIndex, field.dataTypeSize, field.dataOffset);
            record.write(field.name, VarVal::readVarValFromMemory(fieldAddress, field.type, isNull));
        }
        else
        {
            auto fieldAddress = calculateArrowFieldAddress(bufferAddress, recordIndex, field.dataTypeSize, field.dataOffset);
            record.write(field.name, loadValue(field.type, recordBuffer, fieldAddress));
        }
    }
    return record;
}

TupleBufferRef::WriteRecordResult ArrowBufferRef::writeRecord(
    nautilus::val<uint64_t>& recordIndex,
    const RecordBuffer& recordBuffer,
    const Record& rec,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<bool> successful{false};
    nautilus::val<uint64_t> writtenRecords{0};
    if (recordIndex < capacity)
    {
        const auto bufferAddress = recordBuffer.getMemArea();
        for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
        {
            const auto& field = fields.at(i);
            if (not rec.hasField(field.name))
            {
                continue;
            }
            const auto& value = rec.read(field.name);

            if (field.type.nullable)
            {
                writeValidityBit(bufferAddress, field.bitmapOffset, recordIndex, value.isNull());
            }

            if (field.type.type == DataType::Type::BOOLEAN)
            {
                /// BOOLEAN: data is bit-packed — write bit at recordIndex in the data region.
                auto boolVal = value.getRawValueAs<nautilus::val<bool>>();
                auto byteAddr = bitmapByteAddress(bufferAddress, field.dataOffset, recordIndex);
                auto currentByte = readValueFromMemRef<uint8_t>(byteAddr);
                auto bitPos = static_cast<nautilus::val<uint8_t>>(recordIndex % nautilus::val<uint64_t>{8});
                auto mask = nautilus::val<uint8_t>{1} << bitPos;
                auto withBitSet = static_cast<nautilus::val<uint8_t>>(currentByte | mask);
                auto invertedMask = static_cast<nautilus::val<uint8_t>>(mask ^ nautilus::val<uint8_t>{0xFF});
                auto withBitCleared = static_cast<nautilus::val<uint8_t>>(currentByte & invertedMask);
                auto newByte = nautilus::select(boolVal, withBitSet, withBitCleared);
                VarVal{newByte}.writeToMemory(byteAddr);
            }
            else if (field.type.type == DataType::Type::VARSIZED)
            {
                auto fieldAddress = calculateArrowFieldAddress(bufferAddress, recordIndex, field.dataTypeSize, field.dataOffset);
                /// VARSIZED: append string data to the column's child buffer(s), write Arrow offsets.
                /// Child buffers for column with slot S are at indices S, S + numVarSizedCols, S + 2*numVarSizedCols, ...
                const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
                invoke(
                    +[](TupleBuffer* tupleBuffer,
                        AbstractBufferProvider* bufferProvider,
                        int8_t* offsetsEntry,
                        const uint64_t recordIdx,
                        const int8_t* strData,
                        const uint64_t strLen,
                        const uint32_t childSlot,
                        const uint32_t numVarCols,
                        const bool isNull)
                    {
                        auto* offsets = reinterpret_cast<int32_t*>(offsetsEntry);
                        if (recordIdx == 0)
                        {
                            offsets[0] = 0;
                        }

                        /// Null strings have zero length — just advance the offset sentinel and skip data copy
                        const auto effectiveLen = isNull ? uint64_t{0} : strLen;
                        const int32_t startPos = offsets[0];
                        const int32_t endPos = startPos + static_cast<int32_t>(effectiveLen);
                        offsets[1] = endPos;

                        if (effectiveLen == 0)
                        {
                            return;
                        }

                        /// Find the last child buffer for this column, or create the first one.
                        /// Child buffers are interleaved: column S uses indices S, S+numVarCols, S+2*numVarCols, ...
                        const auto totalChildren = tupleBuffer->getNumberOfChildBuffers();
                        uint32_t lastChildIdx = childSlot;
                        bool hasChild = totalChildren > childSlot;
                        if (hasChild)
                        {
                            for (uint32_t idx = childSlot; idx < totalChildren; idx += numVarCols)
                            {
                                lastChildIdx = idx;
                            }
                        }
                        else
                        {
                            /// Create initial child buffers for all columns up to this slot
                            while (tupleBuffer->getNumberOfChildBuffers() <= childSlot)
                            {
                                auto newBuf = bufferProvider->getBufferBlocking();
                                newBuf.setNumberOfTuples(0);
                                [[maybe_unused]] auto idx = tupleBuffer->storeChildBuffer(newBuf);
                            }
                            lastChildIdx = childSlot;
                        }

                        auto childBuf = tupleBuffer->loadChildBuffer(VariableSizedAccess::Index{lastChildIdx});
                        const auto usedBytes = childBuf.getNumberOfTuples();
                        const auto childBufSize = childBuf.getBufferSize();

                        if (usedBytes + effectiveLen > childBufSize)
                        {
                            /// Current child buffer is full — allocate a new one for this column.
                            /// Pad with empty buffers for other columns to maintain stride.
                            const uint32_t nextIdx = lastChildIdx + numVarCols;
                            while (tupleBuffer->getNumberOfChildBuffers() < nextIdx)
                            {
                                auto padBuf = bufferProvider->getBufferBlocking();
                                padBuf.setNumberOfTuples(0);
                                [[maybe_unused]] auto idx = tupleBuffer->storeChildBuffer(padBuf);
                            }

                            /// If the string is larger than a pooled buffer, use an unpooled allocation
                            TupleBuffer newBuf;
                            if (effectiveLen > bufferProvider->getBufferSize())
                            {
                                auto unpooledOpt = bufferProvider->getUnpooledBuffer(effectiveLen);
                                INVARIANT(unpooledOpt.has_value(), "Failed to allocate unpooled buffer of {} bytes for oversized string", effectiveLen);
                                newBuf = std::move(*unpooledOpt);
                            }
                            else
                            {
                                newBuf = bufferProvider->getBufferBlocking();
                            }
                            newBuf.setNumberOfTuples(0);
                            [[maybe_unused]] auto idx = tupleBuffer->storeChildBuffer(newBuf);
                            lastChildIdx = nextIdx;
                            childBuf = tupleBuffer->loadChildBuffer(VariableSizedAccess::Index{lastChildIdx});
                        }

                        const auto writePos = childBuf.getNumberOfTuples();
                        auto dest = childBuf.getAvailableMemoryArea<uint8_t>().data() + writePos;
                        std::memcpy(dest, strData, effectiveLen);
                        childBuf.setNumberOfTuples(writePos + effectiveLen);
                    },
                    recordBuffer.getReference(),
                    bufferProvider,
                    fieldAddress,
                    recordIndex,
                    varSizedValue.getContent(),
                    varSizedValue.getSize(),
                    nautilus::val<uint32_t>{field.childBufferSlot},
                    nautilus::val<uint32_t>{numVarSizedColumns},
                    value.isNull());
            }
            else if (field.type.nullable)
            {
                auto fieldAddress = calculateArrowFieldAddress(bufferAddress, recordIndex, field.dataTypeSize, field.dataOffset);
                const DataType nonNullType(field.type.type, DataType::NULLABLE::NOT_NULLABLE);
                storeValue(nonNullType, recordBuffer, fieldAddress, value, bufferProvider);
            }
            else
            {
                auto fieldAddress = calculateArrowFieldAddress(bufferAddress, recordIndex, field.dataTypeSize, field.dataOffset);
                storeValue(field.type, recordBuffer, fieldAddress, value, bufferProvider);
            }
        }
        writtenRecords = 1;
        successful = true;
    }
    return {.successful = successful, .writtenRecords = writtenRecords};
}

std::vector<Record::RecordFieldIdentifier> ArrowBufferRef::getAllFieldNames() const
{
    return fields | std::views::transform([](const Field& field) { return field.name; }) | std::ranges::to<std::vector>();
}

std::vector<DataType> ArrowBufferRef::getAllDataTypes() const
{
    return fields | std::views::transform([](const Field& field) { return field.type; }) | std::ranges::to<std::vector>();
}

std::vector<TupleBuffer> ArrowBufferRef::loadVarSizedColumnChildBuffers(
    const TupleBuffer& buffer,
    const size_t fieldIndex) const
{
    const auto& field = fields[fieldIndex];
    PRECONDITION(field.childBufferSlot != Field::NO_CHILD_BUFFER, "Field {} is not VARSIZED", fieldIndex);

    std::vector<TupleBuffer> result;
    const auto totalChildren = buffer.getNumberOfChildBuffers();
    for (uint32_t idx = field.childBufferSlot; idx < totalChildren; idx += numVarSizedColumns)
    {
        result.push_back(buffer.loadChildBuffer(VariableSizedAccess::Index{idx}));
    }
    return result;
}

}
