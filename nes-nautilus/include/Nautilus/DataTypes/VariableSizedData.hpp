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
#include <ErrorHandling.hpp>

namespace NES::Nautilus
{

/// Forward declaring the class here, so that we can declare the operator==(const VariableSizedData, const nautilus::val<bool>) for it
class VariableSizedData;
nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other);
nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData);

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
/// This class should not be used as standalone. Rather it should be used via the VarVal class
class VariableSizedData
{
public:
    /// Prevent accidental conversions from nautilus<uint32_t> to nautilus<bool>
    class Owned
    {
        nautilus::val<bool> isOwned;

    public:
        explicit Owned(nautilus::val<bool> isOwned) : isOwned(std::move(isOwned)) { }
        friend class VariableSizedData;
    };

    /// @param bufferBacked: If set to true the VariableSizedData object is backed by a tuple buffer.
    explicit VariableSizedData(const nautilus::val<int8_t*>& reference, const nautilus::val<uint32_t>& size, Owned);
    explicit VariableSizedData(const nautilus::val<int8_t*>& pointerToVarSizedData, Owned);
    VariableSizedData(const VariableSizedData& other);
    VariableSizedData& operator=(const VariableSizedData& other) noexcept;
    VariableSizedData(VariableSizedData&& other) noexcept;
    VariableSizedData& operator=(VariableSizedData&& other) noexcept;


    /// Returns the size of the variable sized data object. This means the size of the size + data
    [[nodiscard]] nautilus::val<uint32_t> getTotalSize() const;

    /// Returns the size of the variable sized data content.
    [[nodiscard]] nautilus::val<uint32_t> getContentSize() const;
    /// Returns the content of the variable sized data, this means the pointer to the actual variable sized data.
    /// In other words, this returns the pointer to the actual data, not the pointer to the size + data
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;

    /// Returns the pointer to the variable sized data, this means the pointer to the size + data
    [[nodiscard]] nautilus::val<int8_t*> getReference() const;

    /// Was the buffer allocated in a owned TupleBuffer
    [[nodiscard]] nautilus::val<bool> ownsBuffer() const;

    nautilus::val<uint32_t> shrink(nautilus::val<uint32_t> bytesToShrink);

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
    nautilus::val<uint32_t> size;
    nautilus::val<int8_t*> ptrToVarSized;
    nautilus::val<bool> ownsBuffer_;
};

template <typename T>
concept NonScalarType = std::same_as<std::remove_cvref_t<T>, VariableSizedData>;

}
