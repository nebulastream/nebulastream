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
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/TextType.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
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
    else if (NES::Util::instanceOf<TextType>(dataType))
    {
        serializedDataType->set_type(SerializableDataType_Type_TEXT);
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: serialization is not possible for " + dataType->toString());
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
    else if (serializedDataType.type() == SerializableDataType_Type_TEXT)
    {
        return std::make_shared<TextType>();
        ;
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: data type which is to be serialized not registered. "
                            "Deserialization is not possible");
}

SerializableDataValue*
DataTypeSerializationUtil::serializeDataValue(const ValueTypePtr& valueType, SerializableDataValue* serializedDataValue)
{
    /// serialize data value
    /// serialize all information for basic value types
    /// 1. cast to BasicValueType
    auto const basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
    assert(basicValueType);
    /// 2. create basic value details
    auto serializedBasicValue = SerializableDataValue_BasicValue();
    /// 3. copy value
    serializedBasicValue.set_value(basicValueType->value);
    serializeDataType(basicValueType->dataType, serializedBasicValue.mutable_type());
    /// 4. serialize basic type
    serializedDataValue->mutable_value()->PackFrom(serializedBasicValue);
    NES_TRACE("DataTypeSerializationUtil:: serialized {} as {} ", valueType->toString(), serializedDataValue->DebugString());
    return serializedDataValue;
}

ValueTypePtr DataTypeSerializationUtil::deserializeDataValue(const SerializableDataValue& serializedDataValue)
{
    /// de-serialize data value
    NES_TRACE("DataTypeSerializationUtil:: de-serialized {}", serializedDataValue.DebugString());
    const auto& dataValue = serializedDataValue.value();
    if (dataValue.Is<SerializableDataValue_BasicValue>())
    {
        auto serializedBasicValue = SerializableDataValue_BasicValue();
        dataValue.UnpackTo(&serializedBasicValue);

        auto const dataTypePtr = deserializeDataType(serializedBasicValue.type());
        return DataTypeFactory::createBasicValue(dataTypePtr, serializedBasicValue.value());
    }
    NES_THROW_RUNTIME_ERROR(
        "DataTypeSerializationUtil: deserialization of value type is not possible: " << serializedDataValue.DebugString());
}

}
