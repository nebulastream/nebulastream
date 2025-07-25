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

#pragma once


#include <functional>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{

class HashMapRef
{
public:
    explicit HashMapRef(const nautilus::val<HashMap*>& hashMapRef) : hashMapRef(hashMapRef) { }

    virtual ~HashMapRef() = default;

    /// This function performs a lookup to the hash map with a record.
    /// If the recordKey is found the entry is returned.
    /// If the key was not found a new entry for the set of keys is inserted and the onInsert function is called.
    /// After the onInsert function is called, the newly-created entry is returned.
    virtual nautilus::val<AbstractHashMapEntry*> findOrCreateEntry(
        const Nautilus::Record& recordKey,
        const HashFunction& hashFunction,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        = 0;

    /// This function inserts an already existing entry from another hash map to this hash map.
    /// To this end, we assume that both hash maps use the same hash function.
    /// If an entry with the same key already exists the onUpdate function is called and allows to merge both entries.
    virtual void insertOrUpdateEntry(
        const nautilus::val<AbstractHashMapEntry*>& otherEntry,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onUpdate,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
        = 0;

protected:
    nautilus::val<HashMap*> hashMapRef;
};

}
