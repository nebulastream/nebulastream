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
#include <cstdint>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES::Nautilus::Interface
{

class AbstractHashMapEntry
{
public:
    virtual ~AbstractHashMapEntry() = default;
};

class HashMap
{
public:
    virtual ~HashMap() = default;
    virtual AbstractHashMapEntry* insertEntry(HashFunction::HashValue::raw_type hash, Memory::AbstractBufferProvider* bufferProvider) = 0;
    [[nodiscard]] virtual uint64_t getNumberOfTuples() const = 0;
};
}
