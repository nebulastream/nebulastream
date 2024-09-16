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

#include <vector>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableDataType.pb.h>
#include <magic_enum.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{

SerializableDataType* DataTypeSerializationUtil::serializeDataType(const DataTypePtr& dataType, SerializableDataType* serializedDataType)
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

DataTypePtr DataTypeSerializationUtil::deserializeDataType(const SerializableDataType& serializedDataType)
{
    NES_TRACE("DataTypeSerializationUtil:: de-serialized {}", serializedDataType.DebugString());
    if (serializedDataType.type() == SerializableDataType_Type_UNDEFINED)
    {
        return DataTypeFactory::createUndefined();
    }
    if (serializedDataType.type() == SerializableDataType_Type_CHAR)
    {
        return DataTypeFactory::createChar();
    }
    else if (serializedDataType.type() == SerializableDataType_Type_INTEGER)
    {
        auto integerDetails = SerializableDataType_IntegerDetails();
        serializedDataType.details().UnpackTo(&integerDetails);
        if (integerDetails.bits() == 64)
        {
            if (integerDetails.lowerbound() == 0)
            {
                return DataTypeFactory::createUInt64();
            }
            else
            {
                return DataTypeFactory::createInt64();
            }
        }
        return DataTypeFactory::createInteger(integerDetails.bits(), integerDetails.lowerbound(), integerDetails.upperbound());
    }
    else if (serializedDataType.type() == SerializableDataType_Type_FLOAT)
    {
        auto floatDetails = SerializableDataType_FloatDetails();
        serializedDataType.details().UnpackTo(&floatDetails);
        if (floatDetails.bits() == 32)
        {
            return DataTypeFactory::createFloat();
        }
        else
        {
            return DataTypeFactory::createDouble();
        }
    }
    else if (serializedDataType.type() == SerializableDataType_Type_BOOLEAN)
    {
        return DataTypeFactory::createBoolean();
    }
    else if (serializedDataType.type() == SerializableDataType_Type_CHAR)
    {
        return DataTypeFactory::createChar();
    }
    else if (serializedDataType.type() == SerializableDataType_Type_VARIABLE_SIZED_DATA)
    {
        return DataTypeFactory::createVariableSizedData();
    }
    throw CannotDeserialize("deserialization is not possible for {}", magic_enum::enum_name(serializedDataType.type()));
}

}
