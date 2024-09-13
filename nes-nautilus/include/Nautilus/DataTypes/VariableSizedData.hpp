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

#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES::Nautilus
{

/// Declaring the class here, so that we can declare the operator==(const VariableSizedData, const nautilus::val<bool>) for it
class VariableSizedData;
nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
/// This class should not be used as standalone. Rather it should be used via the VarVal class
class VariableSizedData
{
public:
    explicit VariableSizedData(const nautilus::val<int8_t*>& content, const nautilus::val<uint32_t>& size);
    explicit VariableSizedData(const nautilus::val<int8_t*>& pointerToVarSizedData);
    VariableSizedData(const VariableSizedData& other);
    VariableSizedData& operator=(const VariableSizedData& other) noexcept;
    VariableSizedData(VariableSizedData&& other) noexcept;
    VariableSizedData& operator=(VariableSizedData&& other) noexcept;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const;
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;
    [[nodiscard]] nautilus::val<int8_t*> getReference() const;

    /// Declaring friend for it, so that we can call VariableSizedData::isValid() in it
    friend std::ostream& operator<<(std::ostream& oss, const VariableSizedData& varSizedData);
    friend nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
    friend nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

    nautilus::val<bool> operator==(const VariableSizedData&) const;
    nautilus::val<bool> operator!=(const VariableSizedData&) const;
    nautilus::val<bool> operator!() const;
    [[nodiscard]] nautilus::val<bool> isValid() const;

private:
    nautilus::val<uint32_t> size;
    nautilus::val<int8_t*> content;
};


}