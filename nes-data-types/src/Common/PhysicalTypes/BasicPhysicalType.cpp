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
#include <string>
#include <utility>
#include <Util/Strings.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{

BasicPhysicalType::BasicPhysicalType(std::shared_ptr<DataType> type, NativeType nativeType)
    : PhysicalType(std::move(type)), nativeType(nativeType)
{
}

std::unique_ptr<PhysicalType> BasicPhysicalType::create(std::shared_ptr<DataType> type, NativeType nativeType)
{
    return std::make_unique<BasicPhysicalType>(std::move(type), nativeType);
}

uint64_t BasicPhysicalType::size() const
{
    switch (nativeType)
    {
        using enum NES::BasicPhysicalType::NativeType;
        case INT_8:
            return sizeof(int8_t);
        case INT_16:
            return sizeof(int16_t);
        case INT_32:
            return sizeof(int32_t);
        case INT_64:
            return sizeof(int64_t);
        case UINT_8:
            return sizeof(uint8_t);
        case UINT_16:
            return sizeof(uint16_t);
        case UINT_32:
            return sizeof(uint32_t);
        case UINT_64:
            return sizeof(uint64_t);
        case FLOAT:
            return sizeof(float);
        case DOUBLE:
            return sizeof(double);
        case BOOLEAN:
            return sizeof(bool);
        case CHAR:
            return sizeof(char);
        case UNDEFINED:
            return -1;
    }
    return -1;
}

std::string BasicPhysicalType::convertRawToString(const void* data) const noexcept
{
    if (!data)
    {
        return std::string();
    }
    switch (nativeType)
    {
        using enum NES::BasicPhysicalType::NativeType;
        case INT_8:
            return std::to_string(*static_cast<const int8_t*>(data));
        case UINT_8:
            return std::to_string(*static_cast<const uint8_t*>(data));
        case INT_16:
            return std::to_string(*static_cast<const int16_t*>(data));
        case UINT_16:
            return std::to_string(*static_cast<const uint16_t*>(data));
        case INT_32:
            return std::to_string(*static_cast<const int32_t*>(data));
        case UINT_32:
            return std::to_string(*static_cast<const uint32_t*>(data));
        case INT_64:
            return std::to_string(*static_cast<const int64_t*>(data));
        case UINT_64:
            return std::to_string(*static_cast<const uint64_t*>(data));
        case FLOAT:
            return Util::formatFloat(*static_cast<const float*>(data));
        case DOUBLE:
            return Util::formatFloat(*static_cast<const double*>(data));
        case BOOLEAN:
            return std::to_string(static_cast<int>(*static_cast<const bool*>(data)));
        case CHAR:
            if (size() != 1)
            {
                return "invalid char type";
            }
            return std::string{*static_cast<const char*>(data)};
        default:
            return "invalid native type";
    }
}

std::string BasicPhysicalType::convertRawToStringWithoutFill(const void* data) const noexcept
{
    if (!data)
    {
        return std::string();
    }
    switch (nativeType)
    {
        using enum NES::BasicPhysicalType::NativeType;
        case INT_8:
            return std::to_string(*static_cast<const int8_t*>(data));
        case UINT_8:
            return std::to_string(*static_cast<const uint8_t*>(data));
        case INT_16:
            return std::to_string(*static_cast<const int16_t*>(data));
        case UINT_16:
            return std::to_string(*static_cast<const uint16_t*>(data));
        case INT_32:
            return std::to_string(*static_cast<const int32_t*>(data));
        case UINT_32:
            return std::to_string(*static_cast<const uint32_t*>(data));
        case INT_64:
            return std::to_string(*static_cast<const int64_t*>(data));
        case UINT_64:
            return std::to_string(*static_cast<const uint64_t*>(data));
        case FLOAT:
            return std::to_string(*static_cast<const float*>(data));
        case DOUBLE:
            return std::to_string(*static_cast<const double*>(data));
        case BOOLEAN:
            return std::to_string(static_cast<int>(*static_cast<const bool*>(data)));
        case CHAR:
            if (size() != 1)
            {
                return "invalid char type";
            }
            return std::string{*static_cast<const char*>(data)};
        default:
            return "invalid native type";
    }
}

std::string BasicPhysicalType::toString() const
{
    switch (nativeType)
    {
        using enum NES::BasicPhysicalType::NativeType;
        case INT_8:
            return "INT8";
        case UINT_8:
            return "UINT8";
        case INT_16:
            return "INT16";
        case UINT_16:
            return "UINT16";
        case INT_32:
            return "INT32";
        case UINT_32:
            return "UINT32";
        case INT_64:
            return "INT64";
        case UINT_64:
            return "UINT64";
        case FLOAT:
            return "FLOAT32";
        case DOUBLE:
            return "FLOAT64";
        case BOOLEAN:
            return "BOOLEAN";
        case CHAR:
            return "CHAR";
        case UNDEFINED:
            return "UNDEFINED";
    }
    return "";
}

std::unique_ptr<PhysicalType> BasicPhysicalType::clone() const
{
    return std::make_unique<BasicPhysicalType>(*this);
}

BasicPhysicalType::BasicPhysicalType(const BasicPhysicalType& other) : PhysicalType(other.type), nativeType(other.nativeType)
{
}

}
