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

#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <nautilus/std/sstream.h>

namespace NES::Nautilus
{

struct FixedScratchMemory
{
    nautilus::val<int8_t*> data = nullptr;
    size_t size = 0;

    bool operator==(const FixedScratchMemory&) const = default;
    operator nautilus::val<bool>() const { return data != nullptr; }
};

struct ScratchMemory
{
    nautilus::val<int8_t*> data = nullptr;
    nautilus::val<size_t> size = 0;

    bool operator==(const ScratchMemory&) const = default;
    operator nautilus::val<bool>() const { return data != nullptr; }
};

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
/// This class should not be used as standalone. Rather it should be used via the VarVal class
class VariableSizedData
{
public:
    explicit VariableSizedData(ScratchMemory memory);

    [[nodiscard]] nautilus::val<size_t> getSize() const;
    /// Returns the content of the variable sized data, this means the pointer to the actual variable sized data.
    /// In other words, this returns the pointer to the actual data, not the pointer to the size + data
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;

    /// Declaring friend for it, so that we can access the members in it and do not have to declare getters for it
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VariableSizedData& variableSizedData);

    /// Performing an equality check between two VariableSizedData objects. Two VariableSizedData objects are equal if their size and
    /// content are byte-wise equal. To check the equality of the content, we compare the content byte-wise via a memcmp.
    friend nautilus::val<bool> operator==(const VariableSizedData& lhs, const VariableSizedData& rhs);

private:
    ScratchMemory memory;
};


}
