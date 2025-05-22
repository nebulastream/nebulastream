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

#include <memory>
#include <string>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

/// The BasicPhysicalType represents nes data types, which can be directly mapped to a native c++ type.
class BasicPhysicalType final : public PhysicalType
{
public:
    enum class NativeType : uint8_t
    {
        UINT_8,
        UINT_16,
        UINT_32,
        UINT_64,
        INT_8,
        INT_16,
        INT_32,
        INT_64,
        FLOAT,
        DOUBLE,
        CHAR,
        BOOLEAN,
        UNDEFINED
    };


    /// Constructor for a basic physical type.
    /// @param type the data type represented by this physical type
    /// @param nativeType the native type of the nes type.
    BasicPhysicalType(std::shared_ptr<DataType> type, NativeType nativeType);

    ~BasicPhysicalType() override = default;

    /// Factory function to create a new physical type.
    /// @param type
    /// @param nativeType
    /// @return std::shared_ptr<PhysicalType>
    static std::shared_ptr<PhysicalType> create(const std::shared_ptr<DataType>& type, NativeType nativeType);

    /// Returns the number of bytes occupied by this data type.
    [[nodiscard]] uint64_t size() const override;

    /// Converts the binary representation of this value to a string.
    /// @param rawData a pointer to the raw value
    std::string convertRawToString(const void* rawData) const noexcept override;


    /// Converts the binary representation of this value to a string.
    /// @param rawData a pointer to the raw value
    std::string convertRawToStringWithoutFill(const void* rawData) const noexcept override;

    [[nodiscard]] std::string toString() const noexcept override;

    NativeType nativeType;
};


}
