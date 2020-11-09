/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <SerializableDataType.pb.h>
#include <Util/Logger.hpp>
#include <vector>
namespace NES {

SerializableDataType* DataTypeSerializationUtil::serializeDataType(DataTypePtr dataType, SerializableDataType* serializedDataType) {
    // serialize data type to the serializedDataType
    if (dataType->isUndefined()) {
        serializedDataType->set_type(SerializableDataType_Type_UNDEFINED);
    } else if (dataType->isInteger()) {
        auto intDataType = DataType::as<Integer>(dataType);
        auto serializedInteger = SerializableDataType_IntegerDetails();
        serializedInteger.set_bits(intDataType->getBits());
        serializedInteger.set_lowerbound(intDataType->getLowerBound());
        serializedInteger.set_upperbound(intDataType->getUpperBound());
        serializedDataType->mutable_details()->PackFrom(serializedInteger);
        serializedDataType->set_type(SerializableDataType_Type_INTEGER);
    } else if (dataType->isFloat()) {
        auto floatDataType = DataType::as<Float>(dataType);
        auto serializableFloat = SerializableDataType_FloatDetails();
        serializableFloat.set_bits(floatDataType->getBits());
        serializableFloat.set_lowerbound(floatDataType->getLowerBound());
        serializableFloat.set_upperbound(floatDataType->getUpperBound());
        serializedDataType->mutable_details()->PackFrom(serializableFloat);
        serializedDataType->set_type(SerializableDataType_Type_FLOAT);
    } else if (dataType->isBoolean()) {
        serializedDataType->set_type(SerializableDataType_Type_BOOLEAN);
    } else if (dataType->isChar()) {
        serializedDataType->set_type(SerializableDataType_Type_CHAR);
    } else if (dataType->isFixedChar()) {
        auto serializableChar = SerializableDataType_CharDetails();
        serializableChar.set_dimensions(DataType::as<FixedChar>(dataType)->getLength());
        serializedDataType->mutable_details()->PackFrom(serializableChar);
        serializedDataType->set_type(SerializableDataType_Type_FIXEDCHAR);
    } else if (dataType->isArray()) {
        // for arrays we store additional information in the SerializableDataType_ArrayDetails
        // 1. cast to array data type
        auto arrayType = DataType::as<Array>(dataType);
        serializedDataType->set_type(SerializableDataType_Type_ARRAY);
        // 2. create details
        auto serializedArray = SerializableDataType_ArrayDetails();
        serializedArray.set_dimensions(arrayType->getLength());
        // 3. serialize the array component type
        serializeDataType(arrayType->getComponent(), serializedArray.mutable_componenttype());
        // 4. store details in serialized object.
        serializedDataType->mutable_details()->PackFrom(serializedArray);
    } else {
        NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: serialization is not possible for " + dataType->toString());
    }
    NES_TRACE("DataTypeSerializationUtil:: serialized " << dataType->toString() << " to " << serializedDataType->SerializeAsString());
    return serializedDataType;
}

DataTypePtr DataTypeSerializationUtil::deserializeDataType(SerializableDataType* serializedDataType) {
    // de-serialize data type to the DataTypePtr
    NES_TRACE("DataTypeSerializationUtil:: de-serialized " << serializedDataType->DebugString());
    if (serializedDataType->type() == SerializableDataType_Type_UNDEFINED) {
        return DataTypeFactory::createUndefined();
    } else if (serializedDataType->type() == SerializableDataType_Type_FIXEDCHAR) {
        auto charDetails = SerializableDataType_CharDetails();
        serializedDataType->details().UnpackTo(&charDetails);
        return DataTypeFactory::createFixedChar(charDetails.dimensions());
    } else if (serializedDataType->type() == SerializableDataType_Type_CHAR) {
        return DataTypeFactory::createChar();
    } else if (serializedDataType->type() == SerializableDataType_Type_INTEGER) {
        auto integerDetails = SerializableDataType_IntegerDetails();
        serializedDataType->details().UnpackTo(&integerDetails);
        return DataTypeFactory::createInteger(integerDetails.bits(), integerDetails.lowerbound(), integerDetails.upperbound());
    } else if (serializedDataType->type() == SerializableDataType_Type_FLOAT) {
        auto floatDetails = SerializableDataType_FloatDetails();
        serializedDataType->details().UnpackTo(&floatDetails);
        return DataTypeFactory::createFloat(floatDetails.bits(), floatDetails.lowerbound(), floatDetails.upperbound());
    } else if (serializedDataType->type() == SerializableDataType_Type_BOOLEAN) {
        return DataTypeFactory::createBoolean();
    } else if (serializedDataType->type() == SerializableDataType_Type_CHAR) {
        return DataTypeFactory::createChar();
    } else if (serializedDataType->type() == SerializableDataType_Type_ARRAY) {
        // for arrays get additional information from the SerializableDataType_ArrayDetails
        auto arrayDetails = SerializableDataType_ArrayDetails();
        serializedDataType->details().UnpackTo(&arrayDetails);
        // get component data type
        auto componentType = deserializeDataType(arrayDetails.release_componenttype());
        return DataTypeFactory::createArray(arrayDetails.dimensions(), componentType);
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: deserialization is not possible");
}

SerializableDataValue* DataTypeSerializationUtil::serializeDataValue(ValueTypePtr valueType, SerializableDataValue* serializedDataValue) {
    // serialize data value
    if (valueType->isArrayValue()) {
        // serialize all information for array value types
        // 1. cast to ArrayValueType
        auto arrayValueType = std::dynamic_pointer_cast<ArrayValue>(valueType);
        // 2. create array value details
        auto serializedArrayValue = SerializableDataValue_ArrayValue();
        // 3. copy array values
        for (const auto& value : arrayValueType->getValues()) {
            serializedArrayValue.add_values(value);
        }
        // 4. serialize array type
        serializeDataType(arrayValueType->getType(), serializedArrayValue.mutable_type());
        serializedDataValue->mutable_value()->PackFrom(serializedArrayValue);
    } else {
        // serialize all information for basic value types
        // 1. cast to BasicValueType
        auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        // 2. create basic value details
        auto serializedBasicValue = SerializableDataValue_BasicValue();
        // 3. copy value
        serializedBasicValue.set_value(basicValueType->getValue());
        serializeDataType(basicValueType->getType(), serializedBasicValue.mutable_type());
        // 4. serialize basic type
        serializedDataValue->mutable_value()->PackFrom(serializedBasicValue);
    }
    NES_TRACE("DataTypeSerializationUtil:: serialized " << valueType->toString() << " as " << serializedDataValue->DebugString());
    return serializedDataValue;
}

ValueTypePtr DataTypeSerializationUtil::deserializeDataValue(SerializableDataValue* serializedDataValue) {
    // de-serialize data value
    NES_TRACE("DataTypeSerializationUtil:: de-serialized " << serializedDataValue->DebugString());
    const auto& dataValue = serializedDataValue->value();
    if (dataValue.Is<SerializableDataValue_BasicValue>()) {
        auto serializedBasicValue = SerializableDataValue_BasicValue();
        dataValue.UnpackTo(&serializedBasicValue);
        auto dataTypePtr = deserializeDataType(serializedBasicValue.release_type());
        return DataTypeFactory::createBasicValue(dataTypePtr, serializedBasicValue.value());
    } else if (dataValue.Is<SerializableDataValue_ArrayValue>()) {
        auto serializedArrayValue = SerializableDataValue_ArrayValue();
        dataValue.UnpackTo(&serializedArrayValue);
        auto dataTypePtr = deserializeDataType(serializedArrayValue.release_type());
        // copy values from serializedArrayValue to array values
        std::vector<std::string> values;
        for (const auto& value : serializedArrayValue.values()) {
            values.emplace_back(value);
        }
        return DataTypeFactory::createArrayValue(dataTypePtr, values);
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: deserialization of value type is not possible: " + serializedDataValue->DebugString());
}

}// namespace NES
