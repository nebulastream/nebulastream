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
#include <Interface/PagedVector/PagedVectorRef.hpp>

#include <bit>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include <cstdint>
#include <ranges>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{
namespace
{
uint64_t getTotalNumberOfRecordsProxy(const TupleBuffer* pagedVectorBuffer)
{
    const PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    return pagedVector.getTotalNumberOfRecords();
}

/// Resolves the page containing `entryPos`, loads its TupleBuffer into `outPageBuffer`, and returns the in-page index.
uint64_t loadPageForEntryProxy(const TupleBuffer* pagedVectorBuffer, const uint64_t entryPos, TupleBuffer* outPageBuffer)
{
    const PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    PRECONDITION(pagedVector.getStatus() == PagedVector::VALID_PV, "Paged Vector must be valid for access.");
    const auto pageIdx = pagedVector.getPageIndex(entryPos);
    *outPageBuffer = pagedVectorBuffer->loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(pageIdx)});
    uint64_t prevCumulativeSum = 0;
    if (pageIdx > 0)
    {
        const auto prevPageBuffer = pagedVectorBuffer->loadChildBuffer(ChildBufferIndex{static_cast<uint32_t>(pageIdx - 1)});
        prevCumulativeSum = PagedVector::Page::load(prevPageBuffer).getCumulativeSum();
    }
    return entryPos - prevCumulativeSum;
}

/// Factory method that returns the lambda function to read a record from a given page. The pageBuffer argument is the capture variable of the page to read from.
/// When the labda is invoked, its argument should be the memory address pointing at the beginning of a record in that page.
auto makeVarSizedLoadFunction(const NautilusBuffer& pageBuffer)
{
    /// NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks,bugprone-exception-escape)
    return [pageBuffer](const nautilus::val<int8_t*>& fieldSlot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(fieldSlot);
        auto varSizedPtr = invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
            +[](TupleBuffer* pageBuffer, const VariableSizedAccess* access) -> int8_t*
            {
                INVARIANT(pageBuffer != nullptr, "Page buffer MUST NOT be null");
                INVARIANT(access != nullptr, "VariableSizedAccess MUST NOT be null");
                auto varSizedPage = pageBuffer->loadChildBuffer(access->getIndex());
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): TupleBuffer hands out int8_t spans by design.
                return reinterpret_cast<int8_t*>(varSizedPage /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
                                                     .getAvailableMemoryArea<>()
                                                     .subspan(access->getOffset().getRawOffset())
                                                     .data());
            },
            pageBuffer.asArg(),
            variableSizedAccess);
        auto varSizedDataLength = invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
            +[](const VariableSizedAccess* access) -> uint64_t
            {
                INVARIANT(access != nullptr, "VariableSizedAccess MUST NOT be null");
                return access->getSize().getRawSize();
            },
            variableSizedAccess);
        return {varSizedPtr, varSizedDataLength}; /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
    };
}

/// Factory method that returns the lambda function to write a record at a specific memory address in a paged vector page.
/// The lastPageBuffer argument is the capture variable pointing to the last page of the paged vector.
/// When the lambda is invoked, its arguments should be the memory pointing to the start where the record will be written to and the record's size.
auto makeVarSizedAllocFunction(const NautilusBuffer& lastPageBuffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
        /// NOLINTNEXTLINE(bugprone-exception-escape): nautilus::invoke ignores the lambda's exception spec; INVARIANT may throw on bad input.
        [lastPageBuffer,
         bufferProvider](const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<uint64_t>& allocationSize) -> nautilus::val<int8_t*>
    {
        return invoke( /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
            +[](TupleBuffer* pageBuffer, AbstractBufferProvider* bufferProvider, int8_t* fieldSlot, uint64_t allocationSize) -> int8_t*
            {
                INVARIANT(pageBuffer != nullptr, "Page buffer must not be null");
                INVARIANT(bufferProvider != nullptr, "BufferProvider must not be null");

                auto numChildren = pageBuffer->getNumberOfChildBuffers();
                if (numChildren > 0)
                {
                    auto lastVarSizedBufferIndex = ChildBufferIndex{static_cast<uint32_t>(numChildren - 1)};
                    TupleBuffer lastVarSizedBuffer = pageBuffer->loadChildBuffer(lastVarSizedBufferIndex);
                    const uint64_t lastVarSizedBufferSize = lastVarSizedBuffer.getBufferSize();
                    const uint64_t lastVarSizedBufferNumTuples = lastVarSizedBuffer.getNumberOfTuples();
                    if (lastVarSizedBufferNumTuples + allocationSize <= lastVarSizedBufferSize)
                    {
                        lastVarSizedBuffer.setNumberOfTuples(allocationSize + lastVarSizedBufferNumTuples);
                        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): fieldSlot is the typed VariableSizedAccess slot.
                        *reinterpret_cast<VariableSizedAccess*>(fieldSlot) = VariableSizedAccess{
                            lastVarSizedBufferIndex,
                            VariableSizedAccess::Offset{lastVarSizedBufferNumTuples},
                            VariableSizedAccess::Size{allocationSize}};
                        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): TupleBuffer hands out int8_t spans by design.
                        return reinterpret_cast<int8_t*>(lastVarSizedBuffer
                                                             .getAvailableMemoryArea<>() /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
                                                             .subspan(lastVarSizedBufferNumTuples)
                                                             .data());
                    }
                }
                TupleBuffer newVarSizedBuffer = bufferProvider->getBufferBlocking();
                auto childIndex = pageBuffer->storeChildBuffer(newVarSizedBuffer);
                newVarSizedBuffer = pageBuffer->loadChildBuffer(childIndex);
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): fieldSlot is the typed VariableSizedAccess slot.
                *reinterpret_cast<VariableSizedAccess*>(fieldSlot)
                    = VariableSizedAccess{childIndex, VariableSizedAccess::Offset{0}, VariableSizedAccess::Size{allocationSize}};
                newVarSizedBuffer.setNumberOfTuples(allocationSize);
                /// NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks,cppcoreguidelines-pro-type-reinterpret-cast)
                return reinterpret_cast<int8_t*>(newVarSizedBuffer.getAvailableMemoryArea<>().data());
            },
            lastPageBuffer.asArg(),
            bufferProvider,
            fieldSlot,
            allocationSize);
    };
}
}

nautilus::val<uint64_t> PagedVectorRef::getNumberOfRecords() const
{
    return nautilus::invoke(getTotalNumberOfRecordsProxy, pagedVectorBuffer.asArg());
}

PagedVectorRef::PagedVectorRef(NautilusBuffer pagedVectorBuffer, std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : pagedVectorBuffer(std::move(pagedVectorBuffer)), tupleLayout(std::move(tupleLayout))
{
}

Record DefaultPagedVectorTupleLayout::readRecord(const nautilus::val<int8_t*> recordMemAddress, LoadVarSizedFunction loadFunction) const
{
    const auto numFields = std::ranges::size(schema);
    Record record;
    uint64_t fieldOffset = 0;
    for (nautilus::static_val<uint64_t> i = 0; i < numFields; ++i)
    {
        auto fieldOpt = schema[i];
        INVARIANT(
            fieldOpt.has_value(),
            "Failed trying to access field at pos {} but schema has only {} fields.",
            static_cast<size_t>(i),
            std::ranges::size(schema));
        const auto name = fieldOpt->getFullyQualifiedName();
        const auto dataType = fieldOpt->getDataType();
        auto fieldAddress = recordMemAddress + nautilus::val<uint64_t>(fieldOffset);

        nautilus::val<bool> null = false;
        nautilus::val<int8_t*> varValRef = fieldAddress;
        if (dataType.nullable)
        {
            null = readValueFromMemRef<bool>(fieldAddress);
            varValRef += 1;
        }
        if (dataType.type != DataType::Type::VARSIZED)
        {
            record.write(name, VarVal::readVarValFromMemory(varValRef, dataType, null));
        }
        else
        {
            auto [ptr, len] = loadFunction(varValRef);
            record.write(name, VarVal{VariableSizedData(ptr, len), dataType.nullable, null});
        }
        fieldOffset += dataType.getSizeInBytesWithNull();
    }
    return record;
}

void DefaultPagedVectorTupleLayout::writeRecord(
    const Record& record, nautilus::val<std::int8_t*> memoryForRecord, AllocateVarSizedFunction allocateVarSized)
{
    const auto numFields = std::ranges::size(schema);
    uint64_t fieldOffset = 0;
    for (nautilus::static_val<uint64_t> i = 0; i < numFields; ++i)
    {
        const auto fieldOpt = schema[i];
        INVARIANT(
            fieldOpt.has_value(),
            "Failed trying to access field at pos {} but schema has only {} fields.",
            static_cast<size_t>(i),
            std::ranges::size(schema));
        const auto name = fieldOpt->getFullyQualifiedName();
        const auto dataType = fieldOpt->getDataType();
        if (not record.hasField(name))
        {
            /// Skipping any fields that are not part of the record
            fieldOffset += dataType.getSizeInBytesWithNull();
            continue;
        }
        auto fieldAddress = memoryForRecord + nautilus::val<uint64_t>(fieldOffset);
        const auto& value = record.read(name);

        /// For now, we store the null byte before the actual VarVal
        nautilus::val<int8_t*> addressToWriteValue = fieldAddress;
        if (dataType.nullable)
        {
            /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
            VarVal{value.isNull()}.writeToMemory(addressToWriteValue);
            addressToWriteValue += 1;
        }
        if (dataType.type != DataType::Type::VARSIZED)
        {
            auto sizeInBytes = nautilus::val<uint64_t>(DataTypeProvider::provideDataType(dataType.type).getSizeInBytesWithNull());

            if (const auto storeFunction = storeValueFunctionMap.find(dataType.type); storeFunction != storeValueFunctionMap.end())
            {
                auto dummy = storeFunction->second(value, addressToWriteValue);
                fieldOffset += dataType.getSizeInBytesWithNull();
                continue;
            }
            throw UnknownDataType("Physical Type: {} is currently not supported", dataType);
        }

        /// field is varsized data, get the appropriate memory address to write it to
        const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
        const nautilus::val<int8_t*> varSizedMemAddress = allocateVarSized(addressToWriteValue, varSizedValue.getSize());

        /// write the varsized data to the memory address
        invoke(
            +[](int8_t* varSizedMemAddress, const int8_t* varSizedDataPtr, const uint64_t varSizedDataLength)
            {
                INVARIANT(varSizedMemAddress != nullptr, "Memory address MUST NOT be null at this point");
                std::memcpy(varSizedMemAddress, varSizedDataPtr, varSizedDataLength);
            },
            varSizedMemAddress,
            varSizedValue.getContent(),
            varSizedValue.getSize());

        fieldOffset += dataType.getSizeInBytesWithNull();
    }
}

void PagedVectorRef::pushBack(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// get the page of the paged vector to write to. Append new page if necessary
    OwnedNautilusBuffer lastPageBuffer;
    invoke(
        +[](TupleBuffer* pagedVectorBuffer, AbstractBufferProvider* bufferProvider, TupleBuffer* currentPage)
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            PRECONDITION(pagedVector.getStatus() == PagedVector::VALID_PV, "Paged Vector must be valid for push_back.");
            pagedVector.appendPageIfFull(bufferProvider);
            auto numPages = pagedVector.getNumberOfPages();
            const ChildBufferIndex lastPageIndex{static_cast<uint32_t>(numPages - 1)};
            *currentPage = pagedVectorBuffer->loadChildBuffer(lastPageIndex);
        },
        pagedVectorBuffer.asArg(),
        bufferProvider,
        lastPageBuffer.asArg());

    /// get the address in the page bufer to write the record to
    auto recordAddress = nautilus::invoke(
        +[](TupleBuffer* pagedVectorBuffer, TupleBuffer* lastPageBuffer) -> int8_t*
        {
            const PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            const auto totalTuplesNum = pagedVector.getTotalNumberOfRecords();
            /// compute index WITHIN the last page
            const uint64_t indexWithinPage = totalTuplesNum % pagedVector.getPageCapacity();
            /// get base address of the page
            const auto pageBufferAddress = lastPageBuffer->getAvailableMemoryArea<>();
            auto* base = std::to_address(pageBufferAddress.begin());
            /// Compute offset safely
            const uint64_t offset = PagedVector::Page::getHeaderSize() + (pagedVector.getTupleSize() * indexWithinPage);
            return std::next(std::bit_cast<int8_t*>(base), static_cast<std::ptrdiff_t>(offset));
        },
        pagedVectorBuffer.asArg(),
        lastPageBuffer.asArg());
    /// write the record using the lambda method from the factory
    tupleLayout->writeRecord(record, recordAddress, makeVarSizedAllocFunction(lastPageBuffer, bufferProvider));
    /// increase the entry count in the page
    nautilus::invoke(
        +[](TupleBuffer* lastPageBuffer) { lastPageBuffer->setNumberOfTuples(lastPageBuffer->getNumberOfTuples() + 1); },
        lastPageBuffer.asArg());
}

Record PagedVectorRef::at(const nautilus::val<uint64_t>& entryPos) const
{
    /// Resolve the page and materialize the page buffer (needed for varsized child loads); return the in-page index.
    OwnedNautilusBuffer pageBuffer;
    auto entryBufferPos = invoke(loadPageForEntryProxy, pagedVectorBuffer.asArg(), entryPos, pageBuffer.asArg());

    auto recordAddress = invoke(
        +[](const TupleBuffer* pagedVectorBuffer, TupleBuffer* pageBuffer, const uint64_t entryBufferPos) -> int8_t*
        {
            const PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            const auto pageBufferAddress = pageBuffer->getAvailableMemoryArea<>();
            auto* base = std::to_address(pageBufferAddress.begin());
            const auto offset = PagedVector::Page::getHeaderSize() + (pagedVector.getTupleSize() * entryBufferPos);
            return std::next(std::bit_cast<int8_t*>(base), static_cast<std::ptrdiff_t>(offset));
        },
        pagedVectorBuffer.asArg(),
        pageBuffer.asArg(),
        entryBufferPos);

    return tupleLayout->readRecord(recordAddress, makeVarSizedLoadFunction(pageBuffer));
}

PagedVectorRefIter PagedVectorRef::begin() const
{
    if (getNumberOfRecords() == 0)
    {
        return PagedVectorRefIter(*this, tupleLayout, OwnedNautilusBuffer{}, 0, 0);
    }

    return PagedVectorRefIter(*this, tupleLayout, pagedVectorBuffer.getChild(0), 0, 0);
}

PagedVectorRefIterSentinel PagedVectorRef::end() const
{
    const auto numberOfTuplesInPagedVector = nautilus::invoke(getTotalNumberOfRecordsProxy, pagedVectorBuffer.asArg());
    return PagedVectorRefIterSentinel(numberOfTuplesInPagedVector);
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector,
    const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
    OwnedNautilusBuffer curPage,
    const nautilus::val<uint64_t>& posOnPage,
    const nautilus::val<uint64_t>& pos)
    : pagedVector(std::move(pagedVector)), pos(pos), posOnPage(posOnPage), curPage(std::move(curPage)), tupleLayout(tupleLayout)
{
}

Record PagedVectorRefIter::operator*() const
{
    auto numberOfRecordsOnPage = curPage.getNumberOfRecords();
    if (posOnPage >= numberOfRecordsOnPage)
    {
        posOnPage = nautilus::invoke(loadPageForEntryProxy, pagedVector.pagedVectorBuffer.asArg(), pos, curPage.asArg());
    }
    /// get the record's address in the page bufer
    auto recordAddress = invoke(
        +[](const TupleBuffer* pagedVectorBuffer, TupleBuffer* pageBuffer, uint64_t entryBufferPos) -> int8_t*
        {
            const PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            const auto pageBufferAddress = pageBuffer->getAvailableMemoryArea<>();
            /// get record address
            auto* base = std::to_address(pageBufferAddress.begin());
            const auto offset = PagedVector::Page::getHeaderSize() + (pagedVector.getTupleSize() * entryBufferPos);
            return std::next(std::bit_cast<int8_t*>(base), static_cast<std::ptrdiff_t>(offset));
        },
        pagedVector.pagedVectorBuffer.asArg(),
        curPage.asArg(),
        posOnPage);
    /// Read the record from the address using the lambda method from the factory
    return tupleLayout->readRecord(recordAddress, makeVarSizedLoadFunction(curPage));
}

PagedVectorRefIter& PagedVectorRefIter::operator++()
{
    pos = pos + 1;
    posOnPage = posOnPage + 1;
    return *this;
}

PagedVectorRefIter PagedVectorRefIter::operator++(int)
{
    auto tmp = *this;
    ++*this;
    return tmp;
}

nautilus::val<bool> PagedVectorRefIter::operator==(const PagedVectorRefIterSentinel& other) const
{
    return this->pos == other.numberOfTuplesInPagedVector;
}

nautilus::val<bool> PagedVectorRefIter::operator!=(const PagedVectorRefIterSentinel& other) const
{
    return not(*this == other);
}

nautilus::val<uint64_t> PagedVectorRefIter::operator-(const PagedVectorRefIter& other) const
{
    return pos - other.pos;
}

PagedVectorRefIterSentinel::PagedVectorRefIterSentinel(const nautilus::val<uint64_t>& numTuplesInPagedVector)
    : numberOfTuplesInPagedVector(numTuplesInPagedVector)
{
}

nautilus::val<bool> PagedVectorRefIterSentinel::operator==(const PagedVectorRefIter& other) const
{
    return other == *this;
}

nautilus::val<bool> PagedVectorRefIterSentinel::operator!=(const PagedVectorRefIter& other) const
{
    return other != *this;
}

}
