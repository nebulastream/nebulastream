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

#include <Interface/BTree/BTreeRef.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/VarSizedStorage.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <NautilusStackAllocation.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
BTreeComparator::BTreeComparator(
    CompilationContext& context,
    std::shared_ptr<BTreeTupleLayout> tupleLayout,
    const std::string& comparatorKey,
    RecordComparator recordComparator)
{
    PRECONDITION(tupleLayout != nullptr, "BTree comparator tuple layout must not be null");
    PRECONDITION(not comparatorKey.empty(), "BTree comparator key must not be empty");
    PRECONDITION(static_cast<bool>(recordComparator), "BTree record comparator must not be empty");

    auto compiledComparator = context.registerFunction(
        std::function(
            [tupleLayout = std::move(tupleLayout), recordComparator = std::move(recordComparator)](
                nautilus::val<TupleBuffer*> buffer,
                nautilus::val<int8_t*> lhs,
                nautilus::val<int8_t*> rhs) /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
            -> nautilus::val<bool>
            {
                const auto btreeBuffer = BorrowedNautilusBuffer::from(buffer);
                const auto loadFunction = makeVarSizedLoadFunction(btreeBuffer);
                return recordComparator(tupleLayout->readRecord(lhs, loadFunction), tupleLayout->readRecord(rhs, loadFunction));
            }),
        std::string{"btreeComparatorFunction:"}.append(comparatorKey));
    comparator = [compiledComparator = std::move(compiledComparator)](TupleBuffer* buffer, int8_t* lhs, int8_t* rhs)
    { return compiledComparator(buffer, lhs, rhs); };
}

bool BTreeComparator::compare(TupleBuffer* treeBuffer, int8_t* lhs, int8_t* rhs) const
{
    return comparator(treeBuffer, lhs, rhs);
}

Record DefaultBTreeTupleLayout::readRecord(const nautilus::val<int8_t*> recordMemAddress, LoadVarSized loadVarSized) const
{
    const auto numFields = std::ranges::size(schema);
    Record record;
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
        const auto fieldAddress = recordMemAddress + nautilus::val<uint64_t>(fieldOffset);
        record.write(name, loadFieldValue(dataType, fieldAddress, loadVarSized));
        fieldOffset += dataType.getSizeInBytesWithNull();
    }
    return record;
}

void DefaultBTreeTupleLayout::writeRecord(const Record& record, nautilus::val<int8_t*> memoryForRecord, StoreVarSized storeVarSized)
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
            fieldOffset += dataType.getSizeInBytesWithNull();
            continue;
        }

        const auto fieldAddress = memoryForRecord + nautilus::val<uint64_t>(fieldOffset);
        storeFieldValue(dataType, fieldAddress, record.read(name), storeVarSized);
        fieldOffset += dataType.getSizeInBytesWithNull();
    }
}

BTreeRef::BTreeRef(NautilusBuffer btreeBuffer, std::shared_ptr<BTreeTupleLayout> tupleLayout)
    : btreeBuffer(std::move(btreeBuffer)), tupleLayout(std::move(tupleLayout))
{
    PRECONDITION(this->tupleLayout != nullptr, "BTree TupleLayout must not be null");
}

Record BTreeRef::read(const nautilus::val<int8_t*>& address) const
{
    return tupleLayout->readRecord(address, makeVarSizedLoadFunction(btreeBuffer));
}

void BTreeRef::appendRecord(
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    const BTreeComparator* comparator,
    const int8_t* entry,
    const uint64_t entrySize)
{
    PRECONDITION(buffer != nullptr, "BTree buffer must not be null");
    PRECONDITION(comparator != nullptr, "BTree comparator must not be null");
    PRECONDITION(entry != nullptr, "BTree entry must not be null");
    auto tree = BTree::load(*buffer);
    tree.append(
        {reinterpret_cast<const std::byte*>(entry), entrySize}, /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        bufferProvider,
        [buffer, comparator](const std::span<const std::byte> lhs, const std::span<const std::byte> rhs)
        {
            return comparator->compare(
                buffer,
                reinterpret_cast<int8_t*>(const_cast<std::byte*>(lhs.data())),
                reinterpret_cast<int8_t*>(const_cast<std::byte*>(rhs.data()))); /// NOLINT(cppcoreguidelines-pro-type-const-cast)
        });
}

void BTreeRef::append(
    const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider, const BTreeComparator& comparator) const
{
    const auto entrySize = tupleLayout->getSizeInBytes();
    const NautilusStackAllocation entry{entrySize};
    tupleLayout->writeRecord(record, entry.data(), makeBTreeVarSizedStoreFunction(btreeBuffer, bufferProvider));

    nautilus::invoke(
        BTreeRef::appendRecord,
        btreeBuffer.asArg(),
        bufferProvider,
        nautilus::val<const BTreeComparator*>{&comparator},
        entry.data(),
        nautilus::val<uint64_t>{entrySize});
}

Record BTreeRef::at(const nautilus::val<uint64_t>& index) const
{
    const auto address = nautilus::invoke(
        +[](const TupleBuffer* buffer, const uint64_t index)
        {
            return reinterpret_cast<int8_t*>(BTree::load(*buffer).at(index).data()); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        },
        btreeBuffer.asArg(),
        index);
    return read(address);
}

nautilus::val<uint64_t> BTreeRef::size() const
{
    return nautilus::invoke(+[](const TupleBuffer* buffer) { return BTree::load(*buffer).size(); }, btreeBuffer.asArg());
}
}
