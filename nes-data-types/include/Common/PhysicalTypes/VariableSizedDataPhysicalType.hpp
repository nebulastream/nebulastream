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
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

/// The VariableSizedDataType physical type, which represent VariableSizedDataType and FixedChar types in NES.
class VariableSizedDataPhysicalType final : public PhysicalType
{
public:
    explicit VariableSizedDataPhysicalType(
        std::shared_ptr<DataType> type, const VariableSizedDataType::Representation representation) noexcept
        : PhysicalType(std::move(type)), representation(representation)
    {
    }

    ~VariableSizedDataPhysicalType() override = default;

    static std::shared_ptr<PhysicalType> create(const std::shared_ptr<DataType>& type) noexcept
    {
        const auto varSizedDataType = std::dynamic_pointer_cast<VariableSizedDataType>(type);
        INVARIANT(varSizedDataType != nullptr, "Variable sized data type is not a variable sized data type.");
        const auto representation = varSizedDataType->representation;
        return std::make_shared<VariableSizedDataPhysicalType>(type, representation);
    }

    [[nodiscard]] uint64_t size() const override;

    std::string convertRawToString(const void* rawData) const noexcept override;

    std::string convertRawToStringWithoutFill(const void* rawData) const noexcept override;

    [[nodiscard]] std::string toString() const noexcept override;

private:
    VariableSizedDataType::Representation representation;
};


}
