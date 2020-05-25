#include <API/Types/DataTypes.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <QueryCompiler/DataTypes/ArrayDataType.hpp>
#include <QueryCompiler/DataTypes/ArrayValueType.hpp>
#include <QueryCompiler/DataTypes/BasicValueType.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Logger.hpp>
#include <vector>
namespace NES {

SerializableDataType* DataTypeSerializationUtil::serializeDataType(DataTypePtr dataType, SerializableDataType* serializedDataType) {
    // serialize data type to the serializedDataType
    if (dataType->isUndefined()) {
        serializedDataType->set_type(SerializableDataType_Type_UNDEFINED);
    } else if (dataType->isEqual(createDataType(CHAR))) {
        serializedDataType->set_type(SerializableDataType_Type_CHAR);
    } else if (dataType->isEqual(createDataType(INT8))) {
        serializedDataType->set_type(SerializableDataType_Type_INT8);
    } else if (dataType->isEqual(createDataType(INT16))) {
        serializedDataType->set_type(SerializableDataType_Type_INT16);
    } else if (dataType->isEqual(createDataType(INT32))) {
        serializedDataType->set_type(SerializableDataType_Type_INT32);
    } else if (dataType->isEqual(createDataType(INT64))) {
        serializedDataType->set_type(SerializableDataType_Type_INT64);
    } else if (dataType->isEqual(createDataType(UINT8))) {
        serializedDataType->set_type(SerializableDataType_Type_UINT8);
    } else if (dataType->isEqual(createDataType(UINT16))) {
        serializedDataType->set_type(SerializableDataType_Type_UINT16);
    } else if (dataType->isEqual(createDataType(UINT32))) {
        serializedDataType->set_type(SerializableDataType_Type_UINT32);
    } else if (dataType->isEqual(createDataType(UINT64))) {
        serializedDataType->set_type(SerializableDataType_Type_UINT64);
    } else if (dataType->isEqual(createDataType(FLOAT32))) {
        serializedDataType->set_type(SerializableDataType_Type_FLOAT32);
    } else if (dataType->isEqual(createDataType(FLOAT64))) {
        serializedDataType->set_type(SerializableDataType_Type_FLOAT64);
    } else if (dataType->isEqual(createDataType(BOOLEAN))) {
        serializedDataType->set_type(SerializableDataType_Type_BOOLEAN);
    } else if (dataType->isArrayDataType()) {
        // for arrays we store additional information in the SerializableDataType_ArrayDetails
        // 1. cast to array data type
        auto arrayType = std::dynamic_pointer_cast<ArrayDataType>(dataType);
        serializedDataType->set_type(SerializableDataType_Type_ARRAY);
        // 2. create details
        auto serializedArray = SerializableDataType_ArrayDetails();
        serializedArray.set_dimensions(arrayType->getDimensions());
        // 3. serialize the array component type
        serializeDataType(arrayType->getComponentDataType(), serializedArray.mutable_componenttype());
        // 4. store details in serialized object.
        serializedDataType->mutable_details()->PackFrom(serializedArray);
    } else {
        NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: serialization is not possible for " + dataType->toString());
    }
    return serializedDataType;
}

DataTypePtr DataTypeSerializationUtil::deserializeDataType(SerializableDataType* serializedDataType) {
    // de-serialize data type to the DataTypePtr
    if (serializedDataType->type() == SerializableDataType_Type_UNDEFINED) {
        return createUndefinedDataType();
    } else if (serializedDataType->type() == SerializableDataType_Type_CHAR) {
        return createDataType(CHAR);
    } else if (serializedDataType->type() == SerializableDataType_Type_INT8) {
        return createDataType(INT8);
    } else if (serializedDataType->type() == SerializableDataType_Type_INT16) {
        return createDataType(INT16);
    } else if (serializedDataType->type() == SerializableDataType_Type_INT32) {
        return createDataType(INT32);
    } else if (serializedDataType->type() == SerializableDataType_Type_INT64) {
        return createDataType(INT64);
    } else if (serializedDataType->type() == SerializableDataType_Type_UINT8) {
        return createDataType(UINT8);
    } else if (serializedDataType->type() == SerializableDataType_Type_UINT16) {
        return createDataType(UINT16);
    } else if (serializedDataType->type() == SerializableDataType_Type_UINT32) {
        return createDataType(UINT32);
    } else if (serializedDataType->type() == SerializableDataType_Type_UINT64) {
        return createDataType(UINT64);
    } else if (serializedDataType->type() == SerializableDataType_Type_FLOAT32) {
        return createDataType(FLOAT32);
    } else if (serializedDataType->type() == SerializableDataType_Type_FLOAT64) {
        return createDataType(FLOAT64);
    } else if (serializedDataType->type() == SerializableDataType_Type_BOOLEAN) {
        return createDataType(BOOLEAN);
    } else if (serializedDataType->type() == SerializableDataType_Type_ARRAY) {
        // for arrays get additional information from the SerializableDataType_ArrayDetails
        auto arrayDetails = SerializableDataType_ArrayDetails();
        serializedDataType->details().UnpackTo(&arrayDetails);
        // get component data type
        auto componentType = deserializeDataType(arrayDetails.release_componenttype());
        return createArrayDataType(componentType, arrayDetails.dimensions());
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: deserialization is not possible");
}

SerializableDataValue* DataTypeSerializationUtil::serializeDataValue(ValueTypePtr valueType, SerializableDataValue* serializedDataValue) {
    // serialize data value
    if (valueType->isArrayValueType()) {
        // serialize all information for array value types
        // 1. cast to ArrayValueType
        auto arrayValueType = std::dynamic_pointer_cast<ArrayValueType>(valueType);
        // 2. create array value details
        auto serializedArrayValue = SerializableDataValue_ArrayValue();
        // 3. copy array values
        for (const auto& value : serializedArrayValue.values()) {
            serializedArrayValue.add_values(value);
        }
        // 4. serialize array type
        serializeDataType(arrayValueType->getType(), serializedArrayValue.mutable_type());
        serializedDataValue->mutable_value()->PackFrom(serializedArrayValue);
    } else {
        // serialize all information for basic value types
        // 1. cast to BasicValueType
        auto basicValueType = std::dynamic_pointer_cast<BasicValueType>(valueType);
        // 2. create basic value details
        auto serializedBasicValue = SerializableDataValue_BasicValue();
        // 3. copy value
        serializedBasicValue.set_value(basicValueType->getValue());
        serializeDataType(basicValueType->getType(), serializedBasicValue.mutable_type());
        // 4. serialize basic type
        serializedDataValue->mutable_value()->PackFrom(serializedBasicValue);
    }
    return serializedDataValue;
}

ValueTypePtr DataTypeSerializationUtil::deserializeDataValue(SerializableDataValue* serializedDataValue) {
    // de-serialize data value
    const auto& dataValue = serializedDataValue->value();
    if (dataValue.Is<SerializableDataValue_BasicValue>()) {
        auto serializedBasicValue = SerializableDataValue_BasicValue();
        dataValue.UnpackTo(&serializedBasicValue);
        auto dataTypePtr = deserializeDataType(serializedBasicValue.release_type());
        // todo replace after reworking the type system
        auto basicDataType = std::dynamic_pointer_cast<BasicDataType>(dataTypePtr)->getType();
        return createBasicTypeValue(basicDataType, serializedBasicValue.value());
    } else if (dataValue.Is<SerializableDataValue_ArrayValue>()) {
        auto serializedArrayValue = SerializableDataValue_ArrayValue();
        dataValue.UnpackTo(&serializedArrayValue);
        auto dataTypePtr = deserializeDataType(serializedArrayValue.release_type());
        // todo replace after reworking the type system
        auto basicDataType = std::dynamic_pointer_cast<BasicDataType>(dataTypePtr)->getType();
        // copy values from serializedArrayValue to array values
        std::vector<std::string> values;
        for (const auto& value : serializedArrayValue.values()) {
            values.emplace_back(value);
        }
        return createArrayValueType(basicDataType, values);
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: deserialization of value type is not possible: " + dataValue.type_url());
}

}// namespace NES
