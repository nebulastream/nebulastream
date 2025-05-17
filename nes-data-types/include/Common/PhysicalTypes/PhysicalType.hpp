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
#include <utility>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{


/// The physical data type represents the physical representation of a NES data type.
class PhysicalType
{
public:
    explicit PhysicalType(std::shared_ptr<DataType> type) noexcept : type(std::move(std::move(type))) { }

    virtual ~PhysicalType() = default;

    /// Returns physical size of type in bytes.
    [[nodiscard]] virtual uint64_t size() const = 0;

    virtual std::string convertRawToString(const void* rawData) const noexcept = 0;

    [[nodiscard]] virtual std::unique_ptr<PhysicalType> clone() const = 0;

    /// Converts the binary representation of this value to a string without filling
    /// up the difference between the length of the string and the end of the schema definition
    /// with unrelated characters.
    virtual std::string convertRawToStringWithoutFill(const void* rawData) const noexcept = 0;
    [[nodiscard]] virtual std::string toString() const = 0;
    bool operator==(const PhysicalType& rhs) const { return *type == *rhs.type; }

    /// Type that is contained by this PhysicalType container
    std::shared_ptr<DataType> type;
};

}
