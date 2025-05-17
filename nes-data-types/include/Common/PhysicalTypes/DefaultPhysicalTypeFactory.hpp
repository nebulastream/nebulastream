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
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>

namespace NES
{

/**
 * @brief This is a default physical type factory, which maps nes types to common x86 types.
 */
class DefaultPhysicalTypeFactory : public PhysicalTypeFactory
{
public:
    DefaultPhysicalTypeFactory();
    ~DefaultPhysicalTypeFactory() override = default;

    /// @brief Translates a nes data type into a corresponding physical type.
    [[nodiscard]] std::unique_ptr<PhysicalType> getPhysicalType(std::shared_ptr<DataType> dataType) const override;

private:
    /// @brief Translates an integer data type into a corresponding physical type.
    static std::unique_ptr<PhysicalType> getPhysicalType(const std::shared_ptr<Integer>& integerType);

    /// @brief Translates a float data type into a corresponding physical type.
    static std::unique_ptr<PhysicalType> getPhysicalType(const std::shared_ptr<Float>& floatType);
};

}
