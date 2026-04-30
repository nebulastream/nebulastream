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
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>

#include <memory>
#include <utility>
#include <vector>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <val_ptr.hpp>

namespace NES
{

uint64_t getTotalNumberOfRecordsProxy(const TupleBuffer* pagedVectorBuffer)
{
    PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    return pagedVector.getTotalNumberOfRecords();
}

uint64_t getBufferPosForEntryProxy(const TupleBuffer* pagedVectorBuffer, const uint64_t entryPos)
{
    PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    return pagedVector.getRecordIndexInPage(entryPos);
}

nautilus::val<uint64_t> PagedVectorRef::getNumberOfRecords() const
{
    return nautilus::invoke(getTotalNumberOfRecordsProxy, pagedVectorBuffer.asArg());
}

PagedVectorRef::PagedVectorRef(NautilusBuffer pagedVectorBuffer, std::shared_ptr<TupleLayout> tupleLayout)
    : pagedVectorBuffer(std::move(pagedVectorBuffer)), tupleLayout(std::move(tupleLayout))
{
}

TupleSchema::TupleSchema(const Schema& schema)
{
    const auto& schemaFields = schema.getFields();
    fields.reserve(schema.getNumberOfFields());
    uint64_t fieldOffset = 0;
    for (const auto& field : schemaFields)
    {
        fields.emplace_back(field.name, field.dataType, fieldOffset);
        fieldOffset += field.dataType.getSizeInBytesWithNull();
    }
}

TupleSchema::Field BasicTupleLayout::getFieldAt(uint64_t pos) const
{
    INVARIANT(
        pos < getNumberOfFields(), "Failed trying to access field at pos {} but schema has only {} fields.", pos, getNumberOfFields());
    return schema.fields[pos];
}

const std::vector<Record::RecordFieldIdentifier> BasicTupleLayout::getAllFieldNames() const
{
    return schema.fields | std::views::transform([](const TupleSchema::Field& field) { return field.name; })
        | std::ranges::to<std::vector>();
}

size_t BasicTupleLayout::getTupleSize() const
{
    size_t totalTupleSize = 0;
    for (const auto& field : schema.fields)
    {
        totalTupleSize += field.dataType.getSizeInBytesWithNull();
    }
    return totalTupleSize;
}

size_t BasicTupleLayout::getNumberOfFields() const
{
    return schema.fields.size();
}

Record BasicTupleLayout::readRecord(const nautilus::val<int8_t*> recordMemAddress, LoadFunction loadFunction) const
{
    const auto& fields = this->getFields();
    Record record;
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, fieldOffset] = fields.at(i);
        auto fieldAddress = recordMemAddress + nautilus::val<uint64_t>(fieldOffset);

        /// For now, we store the null byte before the actual VarVal
        nautilus::val<bool> null = false;
        nautilus::val<int8_t*> varValRef = fieldAddress;
        if (type.nullable)
        {
            /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
            null = readValueFromMemRef<bool>(fieldAddress);
            varValRef += 1;
        }
        if (type.type != DataType::Type::VARSIZED)
        {
            /// Fixed Size field
            auto value = VarVal::readVarValFromMemory(varValRef, type, null);
            record.write(name, value);
            continue;
        }

        /// Variable Size field
        auto [varSizedPtr, varSizedDataLength] = loadFunction(varValRef);
        auto value = VarVal{VariableSizedData(varSizedPtr, varSizedDataLength), type.nullable, null};
        record.write(name, value);
    }
    return record;
}

void BasicTupleLayout::writeRecord(const Record& record, nautilus::val<std::int8_t*> recordMemAddress, AllocateFunction allocateVarSized)
{
    const auto& fields = this->getFields();
    for (nautilus::static_val<uint64_t> i = 0; i < fields.size(); ++i)
    {
        const auto& [name, type, fieldOffset] = fields.at(i);
        if (not record.hasField(name))
        {
            /// Skipping any fields that are not part of the record
            continue;
        }
        auto fieldAddress = recordMemAddress + nautilus::val<uint64_t>(fieldOffset);
        const auto& value = record.read(name);

        /// For now, we store the null byte before the actual VarVal
        nautilus::val<int8_t*> addressToWriteValue = fieldAddress;
        if (type.nullable)
        {
            /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
            VarVal{value.isNull()}.writeToMemory(addressToWriteValue);
            addressToWriteValue += 1;
        }
        if (type.type != DataType::Type::VARSIZED)
        {
            auto sizeInBytes = nautilus::val<uint64_t>(DataTypeProvider::provideDataType(type.type).getSizeInBytesWithNull());

            if (const auto storeFunction = storeValueFunctionMap.find(type.type); storeFunction != storeValueFunctionMap.end())
            {
                auto dummy = storeFunction->second(value, addressToWriteValue);
                continue;
            }
            throw UnknownDataType("Physical Type: {} is currently not supported", type);
        }

        /// field is varsized data, get the appropriate memory address to write it to
        const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
        nautilus::val<int8_t*> varSizedMemAddress = allocateVarSized(addressToWriteValue, varSizedValue.getSize());

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
    }
}

void PagedVectorRef::push_back(const Record& record, nautilus::val<AbstractBufferProvider*> bufferProvider)
{
    /// get the page of the paged vector to write to. Append new page if necessary
    NautilusBuffer lastPageBuffer;
    invoke(
        +[](TupleBuffer* pagedVectorBuffer, AbstractBufferProvider* bufferProvider, TupleBuffer* currentPage)
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            PRECONDITION(pagedVector.getStatus() == PagedVector::VALID_PV, "Paged Vector must be valid for push_back.");
            pagedVector.appendPageIfFull(bufferProvider);
            auto numPages = pagedVector.getNumberOfPages();
            VariableSizedAccess::Index lastPageIndex{numPages - 1};
            *currentPage = pagedVectorBuffer->loadChildBuffer(lastPageIndex);
        },
        pagedVectorBuffer.asArg(),
        bufferProvider,
        lastPageBuffer.asArg());

    /// get the address in the page bufer to write the record to
    auto recordAddress = nautilus::invoke(
        +[](TupleBuffer* pagedVectorBuffer, TupleBuffer* lastPageBuffer) -> int8_t*
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            const auto totalTuplesNum = pagedVector.getTotalNumberOfRecords();
            /// compute index WITHIN the last page
            const uint64_t indexWithinPage = totalTuplesNum % pagedVector.getPageCapacity();
            /// get base address of the page
            const auto pageBufferAddress = lastPageBuffer->getAvailableMemoryArea<>();
            auto* base = std::to_address(pageBufferAddress.begin());
            /// Compute offset safely
            const uint64_t offset = PagedVector::Page::getHeaderSize() + (pagedVector.getTupleSize() * indexWithinPage);
            return reinterpret_cast<int8_t*>(base) + offset;
        },
        pagedVectorBuffer.asArg(),
        lastPageBuffer.asArg());

    /// define varsized memory retrieval/allocation callback
    auto allocateVarSized =
        [lastPageBuffer, bufferProvider](nautilus::val<int8_t*> fieldSlot, nautilus::val<uint64_t> allocationSize) -> nautilus::val<int8_t*>
    {
        return invoke(
            +[](TupleBuffer* pageBuffer, AbstractBufferProvider* bufferProvider, int8_t* fieldSlot, uint64_t allocationSize) -> int8_t*
            {
                INVARIANT(pageBuffer != nullptr, "Page buffer must not be null");
                INVARIANT(bufferProvider != nullptr, "BufferProvider must not be null");

                auto numChildren = pageBuffer->getNumberOfChildBuffers();
                auto lastVarSizedBufferIndex = VariableSizedAccess::Index{numChildren - 1};
                if (numChildren > 0)
                {
                    /// there is a varsized buffer already attached to this page
                    TupleBuffer lastVarSizedBuffer = pageBuffer->loadChildBuffer(lastVarSizedBufferIndex);
                    /// calculate if the varsized data fits
                    const uint64_t lastVarSizedBufferSize = lastVarSizedBuffer.getBufferSize();
                    const uint64_t lastVarSizedBufferNumTuples = lastVarSizedBuffer.getNumberOfTuples();
                    if (lastVarSizedBufferNumTuples + allocationSize < lastVarSizedBufferSize)
                    {
                        /// data fits in the last var sized buffer, no need to allocate a new one
                        /// get the varsized memory address to write to and return a pointer to it
                        lastVarSizedBuffer.setNumberOfTuples(allocationSize + lastVarSizedBufferNumTuples);
                        *reinterpret_cast<VariableSizedAccess*>(fieldSlot) = VariableSizedAccess{
                            lastVarSizedBufferIndex,
                            VariableSizedAccess::Offset{lastVarSizedBufferNumTuples},
                            VariableSizedAccess::Size{allocationSize}};
                        return reinterpret_cast<int8_t*>(
                            lastVarSizedBuffer.getAvailableMemoryArea<>().subspan(lastVarSizedBufferNumTuples).data());
                    }
                }
                /// either no varsized buffers yet or the last varsized buffer can't fit the varsized data to write
                /// allocate new varsized buffer
                TupleBuffer newVarSizedBuffer = bufferProvider->getBufferBlocking();
                auto childIndex = pageBuffer->storeChildBuffer(newVarSizedBuffer);
                newVarSizedBuffer = pageBuffer->loadChildBuffer(childIndex);
                /// in the last paged vector page (at record address), write the index+offset of the varsized data
                *reinterpret_cast<VariableSizedAccess*>(fieldSlot)
                    = VariableSizedAccess{childIndex, VariableSizedAccess::Offset{0}, VariableSizedAccess::Size{allocationSize}};
                newVarSizedBuffer.setNumberOfTuples(allocationSize);
                /// return the memory address for the varsized data
                return reinterpret_cast<int8_t*>(newVarSizedBuffer.getAvailableMemoryArea<>().data());
            },
            lastPageBuffer.asArg(),
            bufferProvider,
            fieldSlot,
            allocationSize);
    };
    /// write the record using the layout
    tupleLayout->writeRecord(record, recordAddress, allocateVarSized);
    /// increase the entry count in the page
    nautilus::invoke(
        +[](TupleBuffer* lastPageBuffer) { lastPageBuffer->setNumberOfTuples(lastPageBuffer->getNumberOfTuples() + 1); },
        lastPageBuffer.asArg());
}

Record PagedVectorRef::at(const nautilus::val<uint64_t>& entryPos) const
{
    /// get the page buffer. We need this because if there record is varsized, the varsized data is stored in child buffers to the page buffer.
    NautilusBuffer pageBuffer;
    invoke(
        +[](TupleBuffer* pagedVectorBuffer, const uint64_t entryPos, TupleBuffer* pageBuffer)
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            PRECONDITION(pagedVector.getStatus() == PagedVector::VALID_PV, "Paged Vector must be valid for access.");
            const auto pageBufferIndex = pagedVector.getPageIndex(entryPos);
            /// load page
            VariableSizedAccess::Index childBufferIndex{pageBufferIndex};
            *pageBuffer = pagedVectorBuffer->loadChildBuffer(childBufferIndex);
        },
        pagedVectorBuffer.asArg(),
        entryPos,
        pageBuffer.asArg());
    /// get the record's address in the page bufer
    auto recordAddress = invoke(
        +[](TupleBuffer* pagedVectorBuffer, TupleBuffer* pageBuffer, const uint64_t entryPos) -> int8_t*
        {
            PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
            auto entryBufferPos = pagedVector.getRecordIndexInPage(entryPos);
            const auto pageBufferAddress = pageBuffer->getAvailableMemoryArea<>();
            /// get record address
            auto* base = std::to_address(pageBufferAddress.begin());
            const auto offset = PagedVector::Page::getHeaderSize() + (pagedVector.getTupleSize() * entryBufferPos);
            return reinterpret_cast<int8_t*>(base) + offset;
        },
        pagedVectorBuffer.asArg(),
        pageBuffer.asArg(),
        entryPos);

    auto loadFunction = [pageBuffer](const nautilus::val<int8_t*>& fieldSlot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(fieldSlot);

        auto varSizedPtr = invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
            +[](TupleBuffer* pageBuffer, const VariableSizedAccess* access) -> int8_t*
            {
                INVARIANT(pageBuffer != nullptr, "Page buffer MUST NOT be null");
                INVARIANT(access != nullptr, "VariableSizedAccess MUST NOT be null");
                auto varSizedPage = pageBuffer->loadChildBuffer(access->getIndex());
                return reinterpret_cast<int8_t*>(
                    varSizedPage.getAvailableMemoryArea<>().subspan(access->getOffset().getRawOffset()).data());
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

        return {varSizedPtr, varSizedDataLength};
    };

    /// Read the record from the address
    return tupleLayout->readRecord(recordAddress, loadFunction);
}

void loadChildBuffer(TupleBuffer* pagedVectorBuffer, uint64_t entryPos, TupleBuffer* currPage)
{
    PagedVector pagedVector = PagedVector::load(*pagedVectorBuffer);
    const auto pageBufferIndex = pagedVector.getPageIndex(entryPos);
    VariableSizedAccess::Index childBufferIndex{pageBufferIndex};
    *currPage = pagedVectorBuffer->loadChildBuffer(childBufferIndex);
}

PagedVectorRefIter PagedVectorRef::begin() const
{
    if (getNumberOfRecords() == 0)
    {
        return PagedVectorRefIter(*this, tupleLayout, NautilusBuffer{}, 0, 0);
    }

    NautilusBuffer curPage;
    nautilus::invoke(loadChildBuffer, pagedVectorBuffer.asArg(), nautilus::val<uint64_t>(0), curPage.asArg());
    return PagedVectorRefIter(*this, tupleLayout, curPage, 0, 0);
}

PagedVectorRefIterSentinel PagedVectorRef::end() const
{
    const auto numberOfTuplesInPagedVector = nautilus::invoke(getTotalNumberOfRecordsProxy, pagedVectorBuffer.asArg());
    return PagedVectorRefIterSentinel(numberOfTuplesInPagedVector);
}

nautilus::val<bool> PagedVectorRef::operator==(const PagedVectorRef& other) const
{
    return tupleLayout == other.tupleLayout && pagedVectorBuffer == other.pagedVectorBuffer;
}

PagedVectorRefIter::PagedVectorRefIter(
    PagedVectorRef pagedVector,
    const std::shared_ptr<TupleLayout>& tupleLayout,
    NautilusBuffer curPage,
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
        posOnPage = 0;
        nautilus::invoke(loadChildBuffer, this->pagedVector.pagedVectorBuffer.asArg(), this->pos, this->curPage.asArg());
        posOnPage = nautilus::invoke(getBufferPosForEntryProxy, this->pagedVector.pagedVectorBuffer.asArg(), this->pos);
    }
    return pagedVector.at(pos);
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

PagedVectorRefIterSentinel::PagedVectorRefIterSentinel(const nautilus::val<uint64_t>& number_of_tuples_in_paged_vector)
    : numberOfTuplesInPagedVector(number_of_tuples_in_paged_vector)
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
