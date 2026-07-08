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

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarArrayData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/VariableSizedAccessRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{

namespace
{
TupleBuffer getNewBufferForVarSized(AbstractBufferProvider& tupleBufferProvider, const uint64_t newBufferSize)
{
    /// If the fixed size buffers are not large enough, we get an unpooled buffer
    if (tupleBufferProvider.getBufferSize() > newBufferSize)
    {
        if (auto newBuffer = tupleBufferProvider.getBufferNoBlocking(); newBuffer.has_value())
        {
            return newBuffer.value();
        }
    }
    const auto unpooledBuffer = tupleBufferProvider.getUnpooledBuffer(newBufferSize);
    if (not unpooledBuffer.has_value())
    {
        throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", newBufferSize);
    }

    return unpooledBuffer.value();
}

/// @brief Copies the varSizedValue to the specified location and then increments the number of tuples
/// @return the new childBufferOffset
void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer, const VariableSizedAccess::Offset childBufferOffset, const std::span<const std::byte> varSizedValue)
{
    const auto spaceInChildBuffer = childBuffer.getAvailableMemoryArea().subspan(childBufferOffset.getRawOffset());
    PRECONDITION(spaceInChildBuffer.size() >= varSizedValue.size(), "SpaceInChildBuffer must be larger than varSizedValue");
    std::ranges::copy(varSizedValue, spaceInChildBuffer.begin());

    /// We increment the number of tuples by the size of the newly added varsized to store the used no. bytes in the tuple buffer.
    /// We plan on getting rid of this "mis"-use in the near future.
    childBuffer.setNumberOfTuples(childBuffer.getNumberOfTuples() + varSizedValue.size());
}
}

VariableSizedAccess TupleBufferRef::writeVarSized(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    const auto totalVarSizedLength = varSizedValue.size();


    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// If there is no space in the lastChildBuffer, we get a new buffer and copy the var sized into the newly acquired
    /// We store the number of used bytes in the no. tuples field.  We plan on getting rid of this "mis"-use in the near future.
    const VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
    if (usedMemorySize + totalVarSizedLength >= lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const VariableSizedAccess::Index childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData(lastChildBuffer, childOffset, varSizedValue);
    return VariableSizedAccess{childIndex, childOffset, VariableSizedAccess::Size{totalVarSizedLength}};
}

std::span<std::byte>
TupleBufferRef::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess) noexcept
{
    /// Loading the childbuffer containing the variable sized data.
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());

    /// Creating a subspan that starts at the required offset. It still can contain multiple other var sized, as we have solely offset the
    /// lower bound but not the upper bound.
    const auto varSized = childBuffer.getAvailableMemoryArea().subspan(variableSizedAccess.getOffset().getRawOffset());

    return varSized.subspan(0, variableSizedAccess.getSize().getRawSize());
}

void TupleBufferRef::inlineVarsizedReferences(
    const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    nautilus::val<int8_t*> varValRef = fieldReference;
    /// Early opt out in case the data type is fixed sized, so all basic types and structs / fixedsized values without varsized elements
    if (!physicalType.isFixedSized())
    {
        switch (physicalType.type)
        {
            case DataType::Type::FIXEDSIZED: {
                /// Iterate over elements
                for (nautilus::static_val<uint32_t> i = 0; i < physicalType.count; ++i)
                {
                    inlineVarsizedReferences(
                        physicalType.elementType[0],
                        recordBuffer,
                        varValRef
                            + nautilus::val<size_t>{
                                i * physicalType.elementType[0].getSizeInBytesWithoutNull()});
                }
                return;
            }
            case DataType::Type::STRUCT: {
                /// Iterate over fields. The counter must be a `static_val` so that the tracer tags the operations of
                /// each unrolled iteration distinctly. With a plain host-side loop every iteration re-enters the
                /// recursion from the same return-address chain, the tags repeat, and the tracer mistakes the
                /// unrolling for a back-edge ("constant loop").
                for (nautilus::static_val<size_t> i = 0; i < physicalType.fields.size(); ++i)
                {
                    const auto& [field, type] = physicalType.fields.at(i);
                    inlineVarsizedReferences(type, recordBuffer, varValRef);
                    varValRef += nautilus::val<size_t>{type.getSizeInBytesWithoutNull()};
                }
                return;
            }
            case DataType::Type::VARSIZED: {
                /// Convert the child buffer reference into a ptr and size and inline them
                auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);
                const auto varSizedPtr = invoke(
                    {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                    +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
                    {
                        INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                        INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                        return loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data();
                    },
                    recordBuffer.getReference(),
                    variableSizedAccess);
                const nautilus::val<uint64_t> size
                    = *getMemberWithOffset<uint64_t>(variableSizedAccess, offsetof(VariableSizedAccess, size));
                const VarVal variableSizedVal{VariableSizedData{varSizedPtr, size}, false, false};
                variableSizedVal.writeToMemory(varValRef);
                return;
            }
            case DataType::Type::VARARRAY: {
                auto varArrayAccess = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);
                const auto varArrayPtr = invoke(
                    {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                    +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
                    {
                        INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                        INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                        return loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data();
                    },
                    recordBuffer.getReference(),
                    varArrayAccess);
                const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(varArrayAccess, offsetof(VariableSizedAccess, size));
                const VarVal varArrayVal{VarArrayData(varArrayPtr, physicalType.elementType[0], size), false, false};
                varArrayVal.writeToMemory(varValRef);
                return;
            }
            default: {
                /// Santity check. The other datatypes should never be deemed as not fixed sized
                INVARIANT(false, "Type {} was deemed as not fixed-sized", magic_enum::enum_name(physicalType.type));
                return;
            }
        }
    }
}

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<bool> null = false;
    nautilus::val<int8_t*> varValRef = fieldReference;
    if (physicalType.nullable)
    {
        /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
        null = readValueFromMemRef<bool>(fieldReference);
        varValRef += 1;
    }
    switch (physicalType.type)
    {
        case DataType::Type::VARSIZED: {
            auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);
            const auto varSizedPtr = invoke(
                {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
                {
                    INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                    INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                    return loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data();
                },
                recordBuffer.getReference(),
                variableSizedAccess);
            const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(variableSizedAccess, offsetof(VariableSizedAccess, size));
            return VarVal{VariableSizedData(varSizedPtr, size), physicalType.nullable, null};
        }
        case DataType::Type::VARARRAY: {
            /// VarArrays are stored identically to VARSIZED values
            auto varArrayAccess = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);
            const auto varArrayPtr = invoke(
                {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
                +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
                {
                    INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                    INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                    return loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data();
                },
                recordBuffer.getReference(),
                varArrayAccess);
            const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(varArrayAccess, offsetof(VariableSizedAccess, size));
            return VarVal{VarArrayData(varArrayPtr, physicalType.elementType[0], size), physicalType.nullable, null};
        }
        case DataType::Type::STRUCT: {
            /// Inline storage: the struct's bytes live directly in the tuple at
            /// `varValRef`. Per-field offsets are determined by `StructData`'s
            /// inline-layout rules.
            /// We need to inline the ptr and size of varsized values before through
            inlineVarsizedReferences(physicalType, recordBuffer, varValRef);
            return VarVal{StructData{varValRef, physicalType.fields}, physicalType.nullable, null};
        }
        case DataType::Type::FIXEDSIZED: {
            /// Like struct, we first inline the ptr and size of every varsized element and then store the array inline
            inlineVarsizedReferences(physicalType, recordBuffer, varValRef);
            return VarVal{FixedSizedData{varValRef, physicalType.count, physicalType.elementType[0]}, physicalType.nullable, null};
        }
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            return VarVal::readVarValFromMemory(varValRef, physicalType, null);
    }
}

VarVal TupleBufferRef::storeValue(
    const DataType& physicalType,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    VarVal value,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<int8_t*> varValRef = fieldReference;
    if (physicalType.nullable)
    {
        /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
        VarVal{value.isNull()}.writeToMemory(varValRef);
        varValRef += 1;
    }
    if (physicalType.type == DataType::Type::STRUCT)
    {
        const auto src = value.getRawValueAs<StructData>();
        if (physicalType.isFixedSized())
        {
            /// Inline storage: copy the struct's bytes straight into the tuple slot.
            /// Width is derived from the schema, so it folds to a constant at trace time.
            const auto bytes = nautilus::val<uint64_t>(physicalType.getSizeInBytesWithoutNull());
            nautilus::memcpy(varValRef, src.getRawPtr(), bytes);
        }
        else
        {
            /// For now write field by field. Later, we can optimize and still cover as many fixedsized fields as possible with one memcpy operation, and only write varsized elements via an extra call
            for (nautilus::static_val<size_t> i = 0; i < src.getNumFields(); ++i)
            {
                const auto& [field, type] = src.getFields().at(i);
                storeValue(type, recordBuffer, varValRef, src.at(i), bufferProvider);
                varValRef += nautilus::val<size_t>{type.getSizeInBytesWithoutNull()};
            }
        }
        return value;
    }
    if (physicalType.type == DataType::Type::FIXEDSIZED)
    {
        /// Fixedsized arrays are stored inline.
        /// However, variable sized element types still need to be represented as child buffer access
        const auto src = value.getRawValueAs<FixedSizedData>();
        if (physicalType.isFixedSized())
        {
            /// Simply inline all the contents
            nautilus::memcpy(varValRef, src.getRawPtr(), nautilus::val<uint64_t>(src.getTotalSizeInBytes()));
        }
        else
        {
            /// Write element per element
            for (nautilus::static_val<size_t> i = 0; i < src.getNumElements(); ++i)
            {
                const nautilus::val<int8_t*> elementReference
                    = varValRef
                    + nautilus::val<size_t>(
                          i * physicalType.elementType[0].getSizeInBytesWithoutNull());

                storeValue(
                    physicalType.elementType[0],
                    recordBuffer,
                    elementReference,
                    src.at(nautilus::val<uint64_t>{i}),
                    bufferProvider);
            }
        }
        return value;
    }

    if (physicalType.type != DataType::Type::VARSIZED && physicalType.type != DataType::Type::VARARRAY)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = storeValueFunctionMap.find(physicalType.type); storeFunction != storeValueFunctionMap.end())
        {
            return storeFunction->second(value, varValRef);
        }
        throw UnknownDataType("Physical Type: {} is currently not supported", physicalType);
    }

    /// VARSIZED, VARARRAY, and FIXEDSIZED both bottom out in `writeVarSized`: it copies the payload
    /// into a child buffer and returns the 16-byte `VariableSizedAccess` for the slot.
    /// Only the source of (pointer, byte-count) differs.
    auto refToIndex = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);

    nautilus::val<int8_t*> payloadPtr{nullptr};
    nautilus::val<uint64_t> payloadLength{0};
    if (physicalType.type == DataType::Type::VARARRAY)
    {
        const auto varsizedArray = value.getRawValueAs<VarArrayData>();
        payloadPtr = varsizedArray.getRawPtr();
        payloadLength = varsizedArray.getTotalSizeInBytes();
    }
    else
    {
        const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
        payloadPtr = varSizedValue.getContent();
        payloadLength = varSizedValue.getSize();
    }

    invoke(
        +[](TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider,
            const int8_t* payloadPtr,
            const uint64_t payloadLength,
            VariableSizedAccess* refToIndex)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            const std::span payloadSpan{payloadPtr, payloadPtr + payloadLength};
            const VariableSizedAccess writtenAccess = writeVarSized(*tupleBuffer, *bufferProvider, std::as_bytes(payloadSpan));
            *refToIndex = writtenAccess;
        },
        recordBuffer.getReference(),
        bufferProvider,
        payloadPtr,
        payloadLength,
        refToIndex);

    return value;
}

bool TupleBufferRef::includesField(
    const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

uint64_t TupleBufferRef::getCapacity() const
{
    return capacity;
}

uint64_t TupleBufferRef::getBufferSize() const
{
    return bufferSize;
}

uint64_t TupleBufferRef::getTupleSize() const
{
    return tupleSize;
}

TupleBufferRef::TupleBufferRef(const uint64_t capacity, const uint64_t bufferSize, const uint64_t tupleSize)
    : capacity(capacity), bufferSize(bufferSize), tupleSize(tupleSize)
{
}

TupleBufferRef::~TupleBufferRef() = default;
}
