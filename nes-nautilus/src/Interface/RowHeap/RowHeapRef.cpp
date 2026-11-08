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

#include <Interface/RowHeap/RowHeapRef.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RowHeap/RowHeap.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

RowHeap::Comparator RowHeapRef::comparatorOf(TupleBuffer* heapBuffer, const BTreeComparator* comparator)
{
    return [heapBuffer, comparator](const int8_t* first, const int8_t* second)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the compiled comparator only reads both rows.
        return comparator->compareInternal(heapBuffer, const_cast<int8_t*>(first), const_cast<int8_t*>(second));
    };
}

RowHeapRef::RowHeapRef(NautilusBuffer heapBuffer, std::shared_ptr<BTreeTupleLayout> tupleLayout)
    : heapBuffer(std::move(heapBuffer)), tupleLayout(std::move(tupleLayout))
{
    PRECONDITION(this->tupleLayout != nullptr, "RowHeap TupleLayout must not be null");
}

std::vector<uint64_t> RowHeapRef::getVarSizedFieldOffsets(const Schema<QualifiedUnboundField, Ordered>& schema)
{
    std::vector<uint64_t> offsets;
    uint64_t fieldOffset = 0;
    for (const auto& field : schema)
    {
        const auto& dataType = field.getDataType();
        if (dataType.type == DataType::Type::VARSIZED)
        {
            /// DefaultBTreeTupleLayout writes the null flag of a nullable field in front of its value.
            offsets.push_back(fieldOffset + (dataType.nullable ? 1 : 0));
        }
        fieldOffset += dataType.getSizeInBytesWithNull();
    }
    return offsets;
}

nautilus::val<uint64_t> RowHeapRef::size() const
{
    return nautilus::invoke(+[](const TupleBuffer* buffer) { return RowHeap::load(*buffer).size(); }, heapBuffer.asArg());
}

Record RowHeapRef::at(const nautilus::val<uint64_t>& index) const
{
    const auto address = nautilus::invoke(
        +[](const TupleBuffer* buffer, const uint64_t index) { return RowHeap::load(*buffer).row(index); }, heapBuffer.asArg(), index);
    const auto loadVarSized = [buffer = heapBuffer](const nautilus::val<int8_t*>& fieldSlot)
    {
        const auto access = static_cast<nautilus::val<VariableSizedAccess*>>(fieldSlot);
        auto data = nautilus::invoke(
            +[](const TupleBuffer* buffer, const VariableSizedAccess* access) -> int8_t*
            {
                auto varSized = buffer->loadChildBuffer(access->getIndex());
                return reinterpret_cast<int8_t*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    varSized.getAvailableMemoryArea<>().subspan(access->getOffset().getRawOffset()).data());
            },
            buffer.asArg(),
            access);
        auto size = nautilus::invoke(+[](const VariableSizedAccess* access) { return access->getSize().getRawSize(); }, access);
        return std::pair{data, size};
    };
    return tupleLayout->readRecord(address, loadVarSized);
}

void RowHeapRef::writePending(const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    const auto pending = nautilus::invoke(+[](TupleBuffer* buffer) { return RowHeap::load(*buffer).clearPending(); }, heapBuffer.asArg());
    const auto storeVarSized = [buffer = heapBuffer, bufferProvider](
                                   const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<int8_t*>& source, const nautilus::val<uint64_t>& size) mutable
    {
        nautilus::invoke(
            +[](TupleBuffer* buffer, AbstractBufferProvider* provider, int8_t* slot, const int8_t* source, const uint64_t size)
            {
                /// The heap may compact its values while storing this one, which updates the accesses of the pending row as well.
                const auto access = RowHeap::load(*buffer).storeVarSized(
                    *provider, std::span{reinterpret_cast<const std::byte*>(source), size}); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                *reinterpret_cast<VariableSizedAccess*>(slot) = access; /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            },
            buffer.asArg(),
            bufferProvider,
            fieldSlot,
            source,
            size);
    };
    tupleLayout->writeRecord(record, pending, storeVarSized);
}

void RowHeapRef::push(
    const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider, const BTreeComparator& comparator) const
{
    writePending(record, bufferProvider);
    nautilus::invoke(
        +[](TupleBuffer* buffer, const BTreeComparator* comparator) { RowHeap::load(*buffer).pushPending(comparatorOf(buffer, comparator)); },
        heapBuffer.asArg(),
        nautilus::val<const BTreeComparator*>{&comparator});
}

void RowHeapRef::replaceTop(
    const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider, const BTreeComparator& comparator) const
{
    writePending(record, bufferProvider);
    nautilus::invoke(
        +[](TupleBuffer* buffer, const BTreeComparator* comparator)
        { RowHeap::load(*buffer).replacePendingTop(comparatorOf(buffer, comparator)); },
        heapBuffer.asArg(),
        nautilus::val<const BTreeComparator*>{&comparator});
}

void RowHeapRef::sortRows(const BTreeComparator& comparator) const
{
    nautilus::invoke(
        +[](TupleBuffer* buffer, const BTreeComparator* comparator) { RowHeap::load(*buffer).sortRows(comparatorOf(buffer, comparator)); },
        heapBuffer.asArg(),
        nautilus::val<const BTreeComparator*>{&comparator});
}

}
