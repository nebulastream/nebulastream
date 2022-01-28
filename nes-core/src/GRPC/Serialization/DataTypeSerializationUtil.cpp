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

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <SerializableDataType.pb.h>
#include <Util/Logger.hpp>
#include <vector>
namespace NES {

SerializableDataType* DataTypeSerializationUtil::serializeDataType(const DataTypePtr& dataType,
                                                                   SerializableDataType* serializedDataType) {
    if (dataType->isUndefined()) {
        serializedDataType->set_type(SerializableDataType_Type_UNDEFINED);
    } else if (dataType->isInteger()) {
        auto intDataType = DataType::as<Integer>(dataType);
        auto serializedInteger = SerializableDataType_IntegerDetails();
        serializedInteger.set_bits(intDataType->getBits());
        serializedInteger.set_lowerbound(intDataType->lowerBound);
        serializedInteger.set_upperbound(intDataType->upperBound);
        serializedDataType->mutable_details()->PackFrom(serializedInteger);
        serializedDataType->set_type(SerializableDataType_Type_INTEGER);
    } else if (dataType->isFloat()) {
        auto floatDataType = DataType::as<Float>(dataType);
        auto serializableFloat = SerializableDataType_FloatDetails();
        serializableFloat.set_bits(floatDataType->getBits());
        serializableFloat.set_lowerbound(floatDataType->lowerBound);
        serializableFloat.set_upperbound(floatDataType->upperBound);
        serializedDataType->mutable_details()->PackFrom(serializableFloat);
        serializedDataType->set_type(SerializableDataType_Type_FLOAT);
    } else if (dataType->isBoolean()) {
        serializedDataType->set_type(SerializableDataType_Type_BOOLEAN);
    } else if (dataType->isChar()) {
        serializedDataType->set_type(SerializableDataType_Type_CHAR);
    } else if (dataType->isArray()) {
        serializedDataType->set_type(SerializableDataType_Type_ARRAY);

        // store dimension and datatype into ArrayDetails by invoking this function recursively.
        auto serializedArray = SerializableDataType_ArrayDetails();
        auto const arrayType = DataType::as<ArrayType>(dataType);
        serializedArray.set_dimensions(arrayType->length);
        serializeDataType(arrayType->component, serializedArray.mutable_componenttype());

        serializedDataType->mutable_details()->PackFrom(serializedArray);
    } else {
        NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: serialization is not possible for " + dataType->toString());
    }
    NES_TRACE("DataTypeSerializationUtil:: serialized " << dataType->toString() << " to "
                                                        << serializedDataType->SerializeAsString());
    return serializedDataType;
}

DataTypePtr DataTypeSerializationUtil::deserializeDataType(SerializableDataType* serializedDataType) {
    NES_TRACE("DataTypeSerializationUtil:: de-serialized " << serializedDataType->DebugString());
    if (serializedDataType->type() == SerializableDataType_Type_UNDEFINED) {
        return DataTypeFactory::createUndefined();
    }
    if (serializedDataType->type() == SerializableDataType_Type_CHAR) {
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
        return deserializeArrayType(serializedDataType);
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: data type which is to be serialized not registered. "
                            "Deserialization is not possible");
    return nullptr;
}

std::shared_ptr<ArrayType> DataTypeSerializationUtil::deserializeArrayType(SerializableDataType* serializedDataType) {
    // for arrays get additional information from the SerializableDataType_ArrayDetails
    auto arrayDetails = SerializableDataType_ArrayDetails();
    serializedDataType->details().UnpackTo(&arrayDetails);

    // get component data type
    auto componentType = deserializeDataType(arrayDetails.release_componenttype());
    return std::make_shared<ArrayType>(arrayDetails.dimensions(), componentType);
}

SerializableDataValue* DataTypeSerializationUtil::serializeDataValue(const ValueTypePtr& valueType,
                                                                     SerializableDataValue* serializedDataValue) {

    // serialize data value
    if (valueType->dataType->isArray()) {
        // serialize all information for array value types
        // 1. cast to ArrayValueType
        auto arrayValueType = std::dynamic_pointer_cast<ArrayValue>(valueType);
        // 2. create array value details
        auto serializedArrayValue = SerializableDataValue_ArrayValue();
        // 3. copy array values
        for (const auto& value : arrayValueType->values) {
            serializedArrayValue.add_values(value);
        }
        // 4. serialize array type
        serializeDataType(arrayValueType->dataType, serializedArrayValue.mutable_type());
        serializedDataValue->mutable_value()->PackFrom(serializedArrayValue);
    } else {
        // serialize all information for basic value types
        // 1. cast to BasicValueType
        auto const basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        assert(basicValueType);
        // 2. create basic value details
        auto serializedBasicValue = SerializableDataValue_BasicValue();
        // 3. copy value
        serializedBasicValue.set_value(basicValueType->value);
        serializeDataType(basicValueType->dataType, serializedBasicValue.mutable_type());
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

        auto* const type = serializedBasicValue.release_type();
        auto const dataTypePtr = deserializeDataType(type);
        return DataTypeFactory::createBasicValue(dataTypePtr, serializedBasicValue.value());
    }
    if (dataValue.Is<SerializableDataValue_ArrayValue>()) {
        auto serializedArrayValue = SerializableDataValue_ArrayValue();
        dataValue.UnpackTo(&serializedArrayValue);

        auto dataTypePtr = deserializeArrayType(serializedArrayValue.release_type());

        // copy values from serializedArrayValue to array values
        std::vector<std::string> values{};
        for (const auto& value : serializedArrayValue.values()) {
            values.emplace_back(std::string{value});
        }
        return DataTypeFactory::createArrayValueFromContainerType(std::move(dataTypePtr), std::move(values));
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: deserialization of value type is not possible: "
                            + serializedDataValue->DebugString());
    return nullptr;
}

}// namespace NES
