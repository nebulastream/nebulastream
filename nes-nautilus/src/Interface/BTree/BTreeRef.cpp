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
#include <type_traits>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
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
namespace
{
struct ExternalVariableSizedAccess
{
    int8_t* data;
    uint64_t size;
};

static_assert(std::is_standard_layout_v<ExternalVariableSizedAccess>);
static_assert(sizeof(ExternalVariableSizedAccess) == sizeof(VariableSizedAccess));

BTreeTupleLayout::LoadVarSized makeBTreeVarSizedLoadFunction(const NautilusBuffer& btreeBuffer)
{
    return [btreeBuffer](const nautilus::val<int8_t*>& fieldSlot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        const auto access = static_cast<nautilus::val<VariableSizedAccess*>>(fieldSlot);
        auto data = nautilus::invoke(
            +[](TupleBuffer* buffer, const VariableSizedAccess* access) -> int8_t*
            {
                INVARIANT(buffer != nullptr, "BTree buffer must not be null");
                INVARIANT(access != nullptr, "BTree VariableSizedAccess must not be null");
                auto page = buffer->loadChildBuffer(access->getIndex());
                return reinterpret_cast<int8_t*>( /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    page.getAvailableMemoryArea<>().subspan(access->getOffset().getRawOffset()).data());
            },
            btreeBuffer.asArg(),
            access);
        auto size = nautilus::invoke(+[](const VariableSizedAccess* access) { return access->getSize().getRawSize(); }, access);
        return {data, size};
    };
}

BTreeTupleLayout::LoadVarSized makeExternalVarSizedLoadFunction()
{
    return [](const nautilus::val<int8_t*>& fieldSlot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        const auto access = static_cast<nautilus::val<ExternalVariableSizedAccess*>>(fieldSlot);
        auto data = nautilus::invoke(+[](const ExternalVariableSizedAccess* value) { return value->data; }, access);
        auto size = nautilus::invoke(+[](const ExternalVariableSizedAccess* value) { return value->size; }, access);
        return {data, size};
    };
}

BTreeTupleLayout::StoreVarSized
makeBTreeVarSizedStoreFunction(const NautilusBuffer& btreeBuffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return [btreeBuffer, bufferProvider](
               const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<int8_t*>& source, const nautilus::val<uint64_t>& size)
    {
        nautilus::invoke(
            +[](TupleBuffer* buffer, AbstractBufferProvider* provider, int8_t* slot, const int8_t* source, const uint64_t size)
            {
                INVARIANT(buffer != nullptr, "BTree buffer must not be null");
                INVARIANT(provider != nullptr, "BTree buffer provider must not be null");
                auto allocation = BTree::load(*buffer).allocateSpaceForVarSized(provider, size);
                *reinterpret_cast<VariableSizedAccess*>(slot) = VariableSizedAccess{/// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                                                                    allocation.childIndex,
                                                                                    VariableSizedAccess::Offset{allocation.offset},
                                                                                    VariableSizedAccess::Size{size}};
                if (size > 0)
                {
                    std::memcpy(allocation.memory.data(), source, size);
                }
            },
            btreeBuffer.asArg(),
            bufferProvider,
            fieldSlot,
            source,
            size);
    };
}

BTreeTupleLayout::StoreVarSized makeExternalVarSizedStoreFunction()
{
    return [](const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<int8_t*>& source, const nautilus::val<uint64_t>& size)
    {
        nautilus::invoke(
            +[](int8_t* slot, int8_t* data, const uint64_t length)
            {
                *reinterpret_cast<ExternalVariableSizedAccess*>(slot)
                    = {data, length}; /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            },
            fieldSlot,
            source,
            size);
    };
}
}

BTreeComparator::BTreeComparator(
    CompilationContext& context,
    std::shared_ptr<BTreeTupleLayout> tupleLayout,
    const std::string& comparatorKey,
    RecordComparator recordComparator)
{
    PRECONDITION(tupleLayout != nullptr, "BTree comparator tuple layout must not be null");
    PRECONDITION(not comparatorKey.empty(), "BTree comparator key must not be empty");
    PRECONDITION(static_cast<bool>(recordComparator), "BTree record comparator must not be empty");

    auto compiledInternalComparator = context.registerFunction(
        std::function(
            [tupleLayout, recordComparator](
                nautilus::val<TupleBuffer*> buffer,
                nautilus::val<int8_t*> lhs,
                nautilus::val<int8_t*> rhs) /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
            -> nautilus::val<bool>
            {
                const auto btreeBuffer = BorrowedNautilusBuffer::from(buffer);
                const auto loadFunction = makeBTreeVarSizedLoadFunction(btreeBuffer);
                return recordComparator(tupleLayout->readRecord(lhs, loadFunction), tupleLayout->readRecord(rhs, loadFunction));
            }),
        std::string{"btreeComparatorFunction:"}.append(comparatorKey));
    internalComparator = [compiledComparator = std::move(compiledInternalComparator)](TupleBuffer* buffer, int8_t* lhs, int8_t* rhs)
    { return compiledComparator(buffer, lhs, rhs); };

    auto compiledExternalComparator = context.registerFunction(
        std::function(
            [tupleLayout = std::move(tupleLayout), recordComparator = std::move(recordComparator)](
                nautilus::val<TupleBuffer*> buffer,
                nautilus::val<int8_t*> internal,
                nautilus::val<int8_t*> external,
                nautilus::val<bool> externalFirst) /// NOLINT(performance-unnecessary-value-param): required by CompilationContext
            -> nautilus::val<bool>
            {
                const auto internalRecord
                    = tupleLayout->readRecord(internal, makeBTreeVarSizedLoadFunction(BorrowedNautilusBuffer::from(buffer)));
                const auto externalRecord = tupleLayout->readRecord(external, makeExternalVarSizedLoadFunction());
                if (externalFirst)
                {
                    return recordComparator(externalRecord, internalRecord);
                }
                return recordComparator(internalRecord, externalRecord);
            }),
        std::string{"btreeExternalComparatorFunction:"}.append(comparatorKey));
    externalComparator = [compiledComparator = std::move(compiledExternalComparator)](
                             TupleBuffer* buffer, int8_t* internal, int8_t* external, const bool externalFirst)
    { return compiledComparator(buffer, internal, external, externalFirst); };
}

bool BTreeComparator::compareInternal(TupleBuffer* treeBuffer, int8_t* lhs, int8_t* rhs) const
{
    return internalComparator(treeBuffer, lhs, rhs);
}

bool BTreeComparator::compareInternalWithExternal(
    TupleBuffer* treeBuffer, int8_t* internal, int8_t* external, const bool externalFirst) const
{
    return externalComparator(treeBuffer, internal, external, externalFirst);
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
        auto fieldAddress = recordMemAddress + nautilus::val<uint64_t>(fieldOffset);

        nautilus::val<bool> null = false;
        nautilus::val<int8_t*> valueAddress = fieldAddress;
        if (dataType.nullable)
        {
            null = readValueFromMemRef<bool>(fieldAddress);
            valueAddress += 1;
        }
        if (dataType.type != DataType::Type::VARSIZED)
        {
            record.write(name, VarVal::readVarValFromMemory(valueAddress, dataType, null));
        }
        else
        {
            auto [ptr, length] = loadVarSized(valueAddress);
            record.write(name, VarVal{VariableSizedData(ptr, length), dataType.nullable, null});
        }
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

        auto fieldAddress = memoryForRecord + nautilus::val<uint64_t>(fieldOffset);
        const auto& value = record.read(name);
        nautilus::val<int8_t*> addressToWriteValue = fieldAddress;
        if (dataType.nullable)
        {
            VarVal{value.isNull()}.writeToMemory(addressToWriteValue);
            addressToWriteValue += 1;
        }
        if (dataType.type != DataType::Type::VARSIZED)
        {
            if (const auto storeFunction = storeValueFunctionMap.find(dataType.type); storeFunction != storeValueFunctionMap.end())
            {
                std::ignore = storeFunction->second(value, addressToWriteValue);
                fieldOffset += dataType.getSizeInBytesWithNull();
                continue;
            }
            throw UnknownDataType("Physical Type: {} is currently not supported", dataType);
        }

        const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
        storeVarSized(addressToWriteValue, varSizedValue.getContent(), varSizedValue.getSize());
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
    return tupleLayout->readRecord(address, makeBTreeVarSizedLoadFunction(btreeBuffer));
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
            return comparator->compareInternal(
                buffer,
                reinterpret_cast<int8_t*>(const_cast<std::byte*>(lhs.data())),
                reinterpret_cast<int8_t*>(const_cast<std::byte*>(rhs.data()))); /// NOLINT(cppcoreguidelines-pro-type-const-cast)
        });
}

uint64_t
BTreeRef::findBound(TupleBuffer* buffer, const BTreeComparator* comparator, const int8_t* entry, const uint64_t entrySize, const bool upper)
{
    PRECONDITION(buffer != nullptr, "BTree buffer must not be null");
    PRECONDITION(comparator != nullptr, "BTree comparator must not be null");
    PRECONDITION(entry != nullptr, "BTree entry must not be null");
    const auto compare = [buffer, comparator, upper](const std::span<const std::byte> lhs, const std::span<const std::byte> rhs)
    {
        auto* const lhsData
            = reinterpret_cast<int8_t*>(const_cast<std::byte*>(lhs.data())); /// NOLINT(cppcoreguidelines-pro-type-const-cast)
        auto* const rhsData
            = reinterpret_cast<int8_t*>(const_cast<std::byte*>(rhs.data())); /// NOLINT(cppcoreguidelines-pro-type-const-cast)
        if (upper)
        {
            return comparator->compareInternalWithExternal(buffer, rhsData, lhsData, true);
        }
        return comparator->compareInternalWithExternal(buffer, lhsData, rhsData, false);
    };
    const auto tree = BTree::load(*buffer);
    const std::span<const std::byte> searchEntry{reinterpret_cast<const std::byte*>(entry), entrySize};
    return upper ? tree.upperBound(searchEntry, compare) : tree.lowerBound(searchEntry, compare);
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

nautilus::val<uint64_t> BTreeRef::findBound(const Record& record, const BTreeComparator& comparator, const bool upper) const
{
    const auto entrySize = tupleLayout->getSizeInBytes();
    const NautilusStackAllocation entry{entrySize};
    nautilus::invoke(
        +[](int8_t* memory, const uint64_t size) { std::memset(memory, 0, size); }, entry.data(), nautilus::val<uint64_t>{entrySize});
    tupleLayout->writeRecord(record, entry.data(), makeExternalVarSizedStoreFunction());
    return nautilus::invoke(
        BTreeRef::findBound,
        btreeBuffer.asArg(),
        nautilus::val<const BTreeComparator*>{&comparator},
        entry.data(),
        nautilus::val<uint64_t>{entrySize},
        nautilus::val<bool>{upper});
}

nautilus::val<uint64_t> BTreeRef::lowerBound(const Record& record, const BTreeComparator& comparator) const
{
    return findBound(record, comparator, false);
}

nautilus::val<uint64_t> BTreeRef::upperBound(const Record& record, const BTreeComparator& comparator) const
{
    return findBound(record, comparator, true);
}

void BTreeRef::initializeIterator(const TupleBuffer* buffer, const uint64_t index, int8_t* state)
{
    PRECONDITION(buffer != nullptr, "BTree buffer must not be null");
    PRECONDITION(state != nullptr, "BTree iterator state must not be null");
    new (state) BTree::Iterator{BTree::load(*buffer).iteratorAt(index)};
}

int8_t* BTreeRef::iteratorAddress(const int8_t* state)
{
    PRECONDITION(state != nullptr, "BTree iterator state must not be null");
    const auto* iterator = reinterpret_cast<const BTree::Iterator*>(state);
    return reinterpret_cast<int8_t*>((*iterator).operator*().data());
}

void BTreeRef::advanceIterator(int8_t* state)
{
    PRECONDITION(state != nullptr, "BTree iterator state must not be null");
    auto* iterator = reinterpret_cast<BTree::Iterator*>(state);
    ++(*iterator);
}

void BTreeRef::destroyIterator(int8_t* state)
{
    PRECONDITION(state != nullptr, "BTree iterator state must not be null");
    std::destroy_at(reinterpret_cast<BTree::Iterator*>(state));
}

BTreeRefIterator BTreeRef::begin() const
{
    return BTreeRefIterator{*this, nautilus::val<uint64_t>{0}};
}

BTreeRefIterator BTreeRef::iteratorAt(const nautilus::val<uint64_t>& index) const
{
    return BTreeRefIterator{*this, index};
}

BTreeRefIteratorSentinel BTreeRef::end() const
{
    return BTreeRefIteratorSentinel{size()};
}

BTreeRefIteratorSentinel BTreeRef::end(const nautilus::val<uint64_t>& index) const
{
    return BTreeRefIteratorSentinel{index};
}

BTreeRefIterator::BTreeRefIterator(BTreeRef tree, const nautilus::val<uint64_t>& position)
    : tree(std::move(tree)), position(position), state(sizeof(BTree::Iterator), alignof(BTree::Iterator))
{
    nautilus::invoke(BTreeRef::initializeIterator, this->tree.btreeBuffer.asArg(), position, state.data());
}

BTreeRefIterator::~BTreeRefIterator()
{
    nautilus::invoke(BTreeRef::destroyIterator, state.data());
}

Record BTreeRefIterator::operator*() const
{
    const auto address = nautilus::invoke(BTreeRef::iteratorAddress, state.data());
    return tree.read(address);
}

BTreeRefIterator& BTreeRefIterator::operator++()
{
    nautilus::invoke(BTreeRef::advanceIterator, state.data());
    position = position + 1;
    return *this;
}

nautilus::val<bool> BTreeRefIterator::operator==(const BTreeRefIteratorSentinel& sentinel) const
{
    return position == sentinel.position;
}

nautilus::val<bool> BTreeRefIterator::operator!=(const BTreeRefIteratorSentinel& sentinel) const
{
    return not(*this == sentinel);
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
