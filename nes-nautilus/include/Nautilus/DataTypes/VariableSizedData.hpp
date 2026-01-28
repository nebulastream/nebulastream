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
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <nautilus/std/sstream.h>
#include <nautilus/val.hpp>

namespace NES
{

/// Forward declaring the class here, so that we can declare the operator==(const VariableSizedData, const nautilus::val<bool>) for it
class VariableSizedData;
nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

/// This class should not be used as standalone. Rather it should be used via the VarVal class
class VariableSizedData
{
public:
    explicit VariableSizedData(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size);
    VariableSizedData(const VariableSizedData& other);
    VariableSizedData& operator=(const VariableSizedData& other) noexcept;
    VariableSizedData(VariableSizedData&& other) noexcept;
    VariableSizedData& operator=(VariableSizedData&& other) noexcept;

    [[nodiscard]] nautilus::val<uint64_t> getSize() const;
    /// Returns the content of the variable sized data, this means the pointer to the actual variable sized data.
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;

    /// Declaring friend for it, so that we can access the members in it and do not have to declare getters for it
    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VariableSizedData& variableSizedData);
    friend nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
    friend nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

    /// Performing an equality check between two VariableSizedData objects. Two VariableSizedData objects are equal if their size and
    /// content are byte-wise equal. To check the equality of the content, we compare the content byte-wise via a memcmp.
    nautilus::val<bool> operator==(const VariableSizedData&) const;
    nautilus::val<bool> operator!=(const VariableSizedData&) const;
    nautilus::val<bool> operator!() const;
    [[nodiscard]] nautilus::val<bool> isValid() const;

private:
    nautilus::val<uint64_t> size;
    nautilus::val<int8_t*> ptrToVarSized;
};


}
