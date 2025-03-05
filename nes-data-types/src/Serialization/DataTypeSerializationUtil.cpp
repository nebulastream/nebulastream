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
#include <vector>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SerializableDataType.pb.h>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{

SerializableDataType*
DataTypeSerializationUtil::serializeDataType(const std::shared_ptr<DataType>& dataType, SerializableDataType* serializedDataType)
{
    if (NES::Util::instanceOf<Undefined>(dataType))
    {
        serializedDataType->set_type(SerializableDataType_Type_UNDEFINED);
    }
    else if (NES::Util::instanceOf<Integer>(dataType))
    {
        auto intDataType = DataType::as<Integer>(dataType);
        auto serializedInteger = SerializableDataType_IntegerDetails();
        serializedInteger.set_bits(intDataType->getBits());
        serializedInteger.set_lowerbound(intDataType->lowerBound);
        serializedInteger.set_upperbound(intDataType->upperBound);
        serializedDataType->mutable_details()->PackFrom(serializedInteger);
        serializedDataType->set_type(SerializableDataType_Type_INTEGER);
    }
    else if (NES::Util::instanceOf<Float>(dataType))
    {
        auto floatDataType = DataType::as<Float>(dataType);
        auto serializableFloat = SerializableDataType_FloatDetails();
        serializableFloat.set_bits(floatDataType->getBits());
        serializableFloat.set_lowerbound(floatDataType->lowerBound);
        serializableFloat.set_upperbound(floatDataType->upperBound);
        serializedDataType->mutable_details()->PackFrom(serializableFloat);
        serializedDataType->set_type(SerializableDataType_Type_FLOAT);
    }
    else if (NES::Util::instanceOf<Boolean>(dataType))
    {
        serializedDataType->set_type(SerializableDataType_Type_BOOLEAN);
    }
    else if (NES::Util::instanceOf<Char>(dataType))
    {
        serializedDataType->set_type(SerializableDataType_Type_CHAR);
    }
    else if (NES::Util::instanceOf<VariableSizedDataType>(dataType))
    {
        serializedDataType->set_type(SerializableDataType_Type_VARIABLE_SIZED_DATA);
    }
    else
    {
        throw CannotSerialize("serialization is not possible for " + dataType->toString());
    }
    NES_TRACE("DataTypeSerializationUtil:: serialized {} to {}", dataType->toString(), serializedDataType->SerializeAsString());
    return serializedDataType;
}

std::shared_ptr<DataType> DataTypeSerializationUtil::deserializeDataType(const SerializableDataType& serializedDataType)
{
    NES_TRACE("DataTypeSerializationUtil:: de-serialized {}", serializedDataType.DebugString());
    const auto nullable = serializedDataType.nullable();
    if (serializedDataType.type() == SerializableDataType_Type_UNDEFINED)
    {
        return DataTypeProvider::provideDataType(LogicalType::UNDEFINED, nullable);
    }
    if (serializedDataType.type() == SerializableDataType_Type_CHAR)
    {
        return DataTypeProvider::provideDataType(LogicalType::CHAR, nullable);
    }
    if (serializedDataType.type() == SerializableDataType_Type_INTEGER)
    {
        auto integerDetails = SerializableDataType_IntegerDetails();
        serializedDataType.details().UnpackTo(&integerDetails);
        if (integerDetails.lowerbound() < 0)
        {
            return DataTypeProvider::provideDataType("INT" + std::to_string(integerDetails.bits()), nullable);
        }
        /// TODO #391: Parsing of string into value should be handled centrally
        return DataTypeProvider::provideDataType("UINT" + std::to_string(integerDetails.bits()), nullable);
    }
    if (serializedDataType.type() == SerializableDataType_Type_FLOAT)
    {
        auto floatDetails = SerializableDataType_FloatDetails();
        serializedDataType.details().UnpackTo(&floatDetails);
        if (floatDetails.bits() == 32)
        {
            return DataTypeProvider::provideDataType(LogicalType::FLOAT32, nullable);
        }
        return DataTypeProvider::provideDataType(LogicalType::FLOAT64, nullable);
    }
    if (serializedDataType.type() == SerializableDataType_Type_BOOLEAN)
    {
        return DataTypeProvider::provideDataType(LogicalType::BOOLEAN, nullable);
    }
    if (serializedDataType.type() == SerializableDataType_Type_CHAR)
    {
        return DataTypeProvider::provideDataType(LogicalType::CHAR, nullable);
    }
    if (serializedDataType.type() == SerializableDataType_Type_VARIABLE_SIZED_DATA)
    {
        return DataTypeProvider::provideDataType(LogicalType::VARSIZED, nullable);
    }
    throw CannotDeserialize("deserialization is not possible for {}", magic_enum::enum_name(serializedDataType.type()));
}

}
