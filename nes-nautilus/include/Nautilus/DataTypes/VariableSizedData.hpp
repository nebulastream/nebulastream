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
#include <optional>
#include <variant>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <nautilus/std/sstream.h>
#include <nautilus/val.hpp>

namespace NES
{

struct ArenaRef;
class VariableSizedData;
nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

namespace detail
{

struct CompoundContent;

/// Content for regular (non-compound) variable sized data
struct RegularContent
{
    nautilus::val<int8_t*> ptr;
    nautilus::val<uint64_t> contentSize;

    RegularContent(const nautilus::val<int8_t*>& content, nautilus::val<uint64_t> size);

    [[nodiscard]] nautilus::val<int8_t*> getContent() const { return ptr; }

    [[nodiscard]] nautilus::val<bool> isValid() const { return ptr != nullptr && contentSize > 0; }

    [[nodiscard]] nautilus::val<bool> operator==(const RegularContent& rhs) const;
    [[nodiscard]] nautilus::val<bool> operator==(const CompoundContent& rhs) const;
};

/// Content for compound (concatenated) variable sized data
struct CompoundContent
{
    ArenaRef* arenaRef;
    nautilus::val<int8_t*> firstPtr;
    nautilus::val<int8_t*> secondPtr;
    nautilus::val<uint64_t> firstSize;
    nautilus::val<uint64_t> secondSize;
    mutable nautilus::val<int8_t*> materializedPtr;

    CompoundContent(
        ArenaRef* arena,
        const nautilus::val<int8_t*>& first,
        nautilus::val<uint64_t> firstSize,
        const nautilus::val<int8_t*>& second,
        nautilus::val<uint64_t> secondSize);

    [[nodiscard]] nautilus::val<int8_t*> getContent() const;

    [[nodiscard]] nautilus::val<bool> isValid() const { return firstPtr != nullptr && secondPtr != nullptr; }

    [[nodiscard]] nautilus::val<bool> operator==(const RegularContent& rhs) const;
    [[nodiscard]] nautilus::val<bool> operator==(const CompoundContent& rhs) const;
};

using ContentVariant = std::variant<RegularContent, CompoundContent>;

}

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
/// This class should not be used as standalone. Rather it should be used via the VarVal class
class VariableSizedData
{
public:
    explicit VariableSizedData(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size);
    VariableSizedData(const VariableSizedData& first, const VariableSizedData& second, ArenaRef& arena);

    VariableSizedData(const VariableSizedData& other) = default;
    VariableSizedData& operator=(const VariableSizedData& other) = default;
    VariableSizedData(VariableSizedData&& other) noexcept = default;
    VariableSizedData& operator=(VariableSizedData&& other) noexcept = default;
    ~VariableSizedData() = default;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const { return size; }

    [[nodiscard]] nautilus::val<int8_t*> getContent() const;
    [[nodiscard]] nautilus::val<int8_t*> getReference() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VariableSizedData& variableSizedData);
    friend nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
    friend nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

    nautilus::val<bool> operator==(const VariableSizedData&) const;
    nautilus::val<bool> operator!=(const VariableSizedData&) const;
    nautilus::val<bool> operator!() const;
    [[nodiscard]] nautilus::val<bool> isValid() const;

protected:
    nautilus::val<uint64_t> size;
    mutable detail::ContentVariant content;
};


}
