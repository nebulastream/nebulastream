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
#include <memory>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>
namespace NES
{

DefaultPhysicalTypeFactory::DefaultPhysicalTypeFactory() = default;

std::unique_ptr<PhysicalType> DefaultPhysicalTypeFactory::getPhysicalType(std::shared_ptr<DataType> dataType) const
{
    if (const auto booleanPtr = std::dynamic_pointer_cast<Boolean>(dataType))
    {
        return BasicPhysicalType::create(dataType, BasicPhysicalType::NativeType::BOOLEAN);
    }
    if (const auto integerPtr = std::dynamic_pointer_cast<Integer>(dataType))
    {
        return getPhysicalType(DataType::as<Integer>(dataType));
    }
    if (const auto floatPtr = std::dynamic_pointer_cast<Float>(dataType))
    {
        return getPhysicalType(DataType::as<Float>(dataType));
    }
    if (const auto charPtr = std::dynamic_pointer_cast<Char>(dataType))
    {
        return BasicPhysicalType::create(dataType, BasicPhysicalType::NativeType::CHAR);
    }
    if (const auto varSizedPtr = std::dynamic_pointer_cast<VariableSizedDataType>(dataType))
    {
        return VariableSizedDataPhysicalType::create(dataType);
    }
    else
    {
        throw UnknownPhysicalType("It was not possible to infer a physical type for: " + dataType->toString());
    }
}


std::unique_ptr<PhysicalType> DefaultPhysicalTypeFactory::getPhysicalType(const std::shared_ptr<Integer>& integer)
{
    using enum NES::BasicPhysicalType::NativeType;
    if (!integer->getIsSigned())
    {
        if (integer->getBits() <= 8)
        {
            return BasicPhysicalType::create(integer, UINT_8);
        }
        else if (integer->getBits() <= 16)
        {
            return BasicPhysicalType::create(integer, UINT_16);
        }
        else if (integer->getBits() <= 32)
        {
            return BasicPhysicalType::create(integer, UINT_32);
        }
        else if (integer->getBits() <= 64)
        {
            return BasicPhysicalType::create(integer, UINT_64);
        }
        else
        {
            throw UnknownPhysicalType("It was not possible to infer a physical type for: " + integer->toString());
        }
    }
    else
    {
        if (integer->getBits() <= 8)
        {
            return BasicPhysicalType::create(integer, INT_8);
        }
        else if (integer->getBits() <= 16)
        {
            return BasicPhysicalType::create(integer, INT_16);
        }
        else if (integer->getBits() <= 32)
        {
            return BasicPhysicalType::create(integer, INT_32);
        }
        else if (integer->getBits() <= 64)
        {
            return BasicPhysicalType::create(integer, INT_64);
        }
        else
        {
            throw UnknownPhysicalType("It was not possible to infer a physical type for: " + integer->toString());
        }
    }
}

std::unique_ptr<PhysicalType> DefaultPhysicalTypeFactory::getPhysicalType(const std::shared_ptr<Float>& floatType)
{
    if (floatType->getBits() <= 32)
    {
        return BasicPhysicalType::create(floatType, BasicPhysicalType::NativeType::FLOAT);
    }
    else if (floatType->getBits() <= 64)
    {
        return BasicPhysicalType::create(floatType, BasicPhysicalType::NativeType::DOUBLE);
    }
    else
    {
        throw UnknownPhysicalType("It was not possible to infer a physical type for: " + floatType->toString());
    }
}

}
