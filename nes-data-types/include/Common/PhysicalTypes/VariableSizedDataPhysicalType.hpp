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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

/// The VariableSizedDataType physical type, which represent VariableSizedDataType and FixedChar types in NES.
class VariableSizedDataPhysicalType final : public PhysicalType
{
public:
    explicit VariableSizedDataPhysicalType(std::shared_ptr<DataType> type) noexcept : PhysicalType(std::move(type)) { }

    ~VariableSizedDataPhysicalType() override = default;

    static std::shared_ptr<PhysicalType> create(const std::shared_ptr<DataType>& type) noexcept
    {
        return std::make_shared<VariableSizedDataPhysicalType>(type);
    }

    std::string convertRawToString(const void* rawData) const noexcept override;

    std::string convertRawToStringWithoutFill(const void* rawData) const noexcept override;

    [[nodiscard]] std::string toString() const noexcept override;
protected:
    [[nodiscard]] uint64_t getRawSizeInBytes() const noexcept override;

private:
    static constexpr size_t sizeVal = sizeof(uint32_t);
};


}
