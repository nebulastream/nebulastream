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

#include <DataStructures/HashMap.hpp>

#include "Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp"

struct IteratorBuffer
{
    uint32_t bucketIndex;
    uint32_t entryIndex;
    NES::TupleBuffer* map;
    HashMap::StoragePageHeader* page;
};

struct EntryBuffer
{
    const std::byte* key;
    const std::byte* value;
};

thread_local EntryBuffer entryBuffer_;

nautilus::val<EntryBuffer*> entryBuffer()
{
    return nautilus::val<EntryBuffer*>(&entryBuffer_);
}

thread_local IteratorBuffer iteratorBuffer_;

nautilus::val<IteratorBuffer*> iteratorBuffer()
{
    return nautilus::val<IteratorBuffer*>(&iteratorBuffer_);
}

nautilus::val<int8_t*> scratch()
{
    thread_local int8_t scratch[1024];
    return nautilus::val<int8_t*>(scratch);
}

#define member_access(value, member) \
    (static_cast<nautilus::val<std::add_pointer_t<decltype(std::declval<decltype(value)::ValType>().member)>>>( \
        static_cast<nautilus::val<int8_t*>>(value) + nautilus::val<size_t>(__builtin_offsetof(decltype(value)::ValType, member))))

bool hashMapInsert(
    NES::TupleBuffer* buffer, int8_t* key, size_t keySize, int8_t* value, size_t valueSize, NES::AbstractBufferProvider* bufferProvider)
{
    auto map = HashMap::load(*buffer);
    return map.insert(
        std::span{reinterpret_cast<std::byte*>(key), keySize},
        std::span{reinterpret_cast<std::byte*>(value), valueSize},
        *bufferProvider,
        [](const auto& key) { return std::hash<std::string_view>{}({reinterpret_cast<const char*>(key.data()), key.size()}); });
}

const int8_t* hashMapGet(NES::TupleBuffer* buffer, int8_t* key, size_t keySize)
{
    auto map = HashMap::load(*buffer);
    auto result = map.get(
        std::span{reinterpret_cast<const std::byte*>(key), keySize},
        [](const auto& key) { return std::hash<std::string_view>{}({reinterpret_cast<const char*>(key.data()), key.size()}); });

    if (result)
    {
        return reinterpret_cast<const int8_t*>(result->data());
    }
    else
    {
        return nullptr;
    }
}

nautilus::val<bool> HashMapRef::insert(const Key& key, const Value& value, nautilus::val<NES::AbstractBufferProvider*> bufferProvider)
{
    size_t usedScratch = 0;
    for (nautilus::static_val<size_t> index = 0; index < key.keys.size(); ++index)
    {
        key.keys[index].writeToMemory(scratch() + nautilus::val<size_t>(usedScratch));
        usedScratch += key.types.getFieldAt(index).dataType.getRawSizeInBytes();
    }
    nautilus::val<size_t> keySize = usedScratch;
    for (nautilus::static_val<size_t> index = 0; index < value.values.size(); ++index)
    {
        value.values[index].writeToMemory(scratch() + nautilus::val<size_t>(usedScratch));
        usedScratch += value.types.getFieldAt(index).dataType.getRawSizeInBytes();
    }
    nautilus::val<size_t> valueSize = usedScratch - keySize;

    return nautilus::invoke(hashMapInsert, buffer, scratch(), keySize, scratch() + keySize, valueSize, bufferProvider);
}

nautilus::val<bool> HashMapRef::get(const Key& key, Value& value)
{
    nautilus::val<size_t> usedScratch = 0;
    for (nautilus::static_val<size_t> index = 0; index < key.keys.size(); ++index)
    {
        key.keys[index].writeToMemory(scratch() + usedScratch);
        usedScratch += key.types.getFieldAt(index).dataType.getRawSizeInBytes();
    }
    auto entry = nautilus::invoke(hashMapGet, buffer, scratch(), usedScratch);
    for (nautilus::static_val<size_t> index = 0; index < value.types.getNumberOfFields(); ++index)
    {
        value.values.emplace_back(NES::VarVal{0}.castToType(value.types.getFieldAt(index).dataType.type));
    }

    if (entry != nullptr)
    {
        size_t offset = 0;
        for (nautilus::static_val<size_t> index = 0; index < value.types.getNumberOfFields(); ++index)
        {
            value.values[index] = NES::VarVal::readVarValFromMemory(entry + offset, value.types.getFieldAt(index).dataType);
            offset += value.types.getFieldAt(index).dataType.getRawSizeInBytes();
        }
    }

    return entry != nullptr;
}

void hashMapBegin(NES::TupleBuffer* buffer, IteratorBuffer* iterator)
{
    auto map = HashMap::load(*buffer);
    auto it = map.begin();
    iterator->map = buffer;
    iterator->page = it.page.storagePage;
    iterator->entryIndex = it.entryIndex;
    iterator->bucketIndex = it.bucketIndex;
}

void hashMapIncrement(IteratorBuffer* iterator)
{
    auto map = HashMap::load(*iterator->map);
    HashMap::Iterator actualIterator{&map, iterator->bucketIndex, HashMap::Page{iterator->page}, iterator->entryIndex};
    ++actualIterator;
    iterator->bucketIndex = actualIterator.bucketIndex;
    iterator->entryIndex = actualIterator.entryIndex;
    iterator->page = actualIterator.page.storagePage;
    if (actualIterator.map == nullptr)
    {
        iterator->map = nullptr;
        iterator->bucketIndex = 0;
        iterator->entryIndex = 0;
        iterator->page = nullptr;
    }
}

void hashMapDeref(IteratorBuffer* iterator, EntryBuffer* entry)
{
    auto map = HashMap::load(*iterator->map);
    HashMap::Iterator actualIterator{&map, iterator->bucketIndex, HashMap::Page{iterator->page}, iterator->entryIndex};
    auto [key, value] = *actualIterator;
    entry->key = key.data();
    entry->value = key.data();
}

HashMapRef::Iterator::Iterator(nautilus::val<NES::TupleBuffer*> map, NES::Schema keyTypes, NES::Schema valueTypes)
    : keyTypes(std::move(keyTypes)), valueTypes(std::move(valueTypes)), map(map)
{
    if (map != nullptr)
    {
        nautilus::invoke(hashMapBegin, map, iteratorBuffer());
        this->bucketIndex = *member_access(iteratorBuffer(), bucketIndex);
        this->page = *member_access(iteratorBuffer(), page);
        this->map = *member_access(iteratorBuffer(), map);
        this->entryIndex = *member_access(iteratorBuffer(), entryIndex);
    }
}

HashMapRef::Iterator::reference HashMapRef::Iterator::operator*() const
{
    Entry result{};
    result.values.reserve(valueTypes.getNumberOfFields());
    result.keys.reserve(keyTypes.getNumberOfFields());

    *member_access(iteratorBuffer(), bucketIndex) = this->bucketIndex;
    *member_access(iteratorBuffer(), page) = this->page;
    *member_access(iteratorBuffer(), map) = this->map;
    *member_access(iteratorBuffer(), entryIndex) = this->entryIndex;
    nautilus::invoke(hashMapDeref, iteratorBuffer(), entryBuffer());

    auto keyPtr = member_access(entryBuffer(), key);
    auto valuePtr = member_access(entryBuffer(), value);

    nautilus::val<size_t> offset = 0;
    for (nautilus::static_val<size_t> index = 0; index < keyTypes.getNumberOfFields(); ++index)
    {
        result.keys.emplace_back(NES::VarVal::readVarValFromMemory(keyPtr + offset, keyTypes.getFieldAt(index).dataType));
        offset += keyTypes.getFieldAt(index).dataType.getRawSizeInBytes();
    }

    offset = 0;
    for (nautilus::static_val<size_t> index = 0; index < valueTypes.getNumberOfFields(); ++index)
    {
        result.values.emplace_back(NES::VarVal::readVarValFromMemory(valuePtr + offset, valueTypes.getFieldAt(index).dataType));
        offset += valueTypes.getFieldAt(index).dataType.getRawSizeInBytes();
    }

    return result;
}

HashMapRef::Iterator& HashMapRef::Iterator::operator++()
{
    *member_access(iteratorBuffer(), bucketIndex) = this->bucketIndex;
    *member_access(iteratorBuffer(), page) = this->page;
    *member_access(iteratorBuffer(), map) = this->map;
    *member_access(iteratorBuffer(), entryIndex) = this->entryIndex;
    nautilus::invoke(hashMapIncrement, iteratorBuffer());

    this->bucketIndex = *member_access(iteratorBuffer(), bucketIndex);
    this->page = *member_access(iteratorBuffer(), page);
    this->map = *member_access(iteratorBuffer(), map);
    this->entryIndex = *member_access(iteratorBuffer(), entryIndex);
    return *this;
}