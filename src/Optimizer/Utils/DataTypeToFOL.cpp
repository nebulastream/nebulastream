#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Optimizer/Utils/DataTypeToFOL.hpp>
#include <Util/Logger.hpp>
#include <string.h>
#include <z3++.h>

namespace NES {

z3::expr NES::DataTypeToFOL::serializeDataType(std::string fieldName, DataTypePtr dataType, z3::context& context) {
    // serialize data type to the serializedDataType
    if (dataType->isUndefined()) {
        //TODO: Not sure how to solve this
    } else if (dataType->isInteger()) {
        return context.int_const(fieldName.c_str());

        //        auto serializedInteger = SerializableDataType_IntegerDetails();
        //        serializedInteger.set_bits(intDataType->getBits());
        //        serializedInteger.set_lowerbound(intDataType->getLowerBound());
        //        serializedInteger.set_upperbound(intDataType->getUpperBound());
        //        serializedDataType->mutable_details()->PackFrom(serializedInteger);
        //        serializedDataType->set_type(SerializableDataType_Type_INTEGER);
    } else if (dataType->isFloat()) {
        return context.fpa_const<64>(fieldName.c_str());
        auto floatDataType = DataType::as<Float>(dataType);
        //        auto serializableFloat = SerializableDataType_FloatDetails();
        //        serializableFloat.set_bits(floatDataType->getBits());
        //        serializableFloat.set_lowerbound(floatDataType->getLowerBound());
        //        serializableFloat.set_upperbound(floatDataType->getUpperBound());
        //        serializedDataType->mutable_details()->PackFrom(serializableFloat);
        //        serializedDataType->set_type(SerializableDataType_Type_FLOAT);
    } else if (dataType->isBoolean()) {
        return context.bool_const(fieldName.c_str());
        //        serializedDataType->set_type(SerializableDataType_Type_BOOLEAN);
    } else if (dataType->isChar()) {
        return context.string_val(fieldName.c_str());
        //        serializedDataType->set_type(SerializableDataType_Type_CHAR);
    } else if (dataType->isFixedChar()) {
        //        auto serializableChar = SerializableDataType_CharDetails();
        //        serializableChar.set_dimensions(DataType::as<FixedChar>(dataType)->getLength());
        //        serializedDataType->mutable_details()->PackFrom(serializableChar);
        //        serializedDataType->set_type(SerializableDataType_Type_FIXEDCHAR);
    } else if (dataType->isArray()) {
        // for arrays we store additional information in the SerializableDataType_ArrayDetails
        // 1. cast to array data type
        auto arrayType = DataType::as<Array>(dataType);
        //        serializedDataType->set_type(SerializableDataType_Type_ARRAY);
        // 2. create details
        //        auto serializedArray = SerializableDataType_ArrayDetails();
        //        serializedArray.set_dimensions(arrayType->getLength());
        // 3. serialize the array component type
        //        serializeDataType(arrayType->getComponent(), serializedArray.mutable_componenttype());
        // 4. store details in serialized object.
        //        serializedDataType->mutable_details()->PackFrom(serializedArray);
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: serialization is not possible for " + dataType->toString());
}

z3::expr DataTypeToFOL::serializeDataValue(ValueTypePtr valueType, z3::context& context) {
    // serialize data value
    if (valueType->isArrayValue()) {
        // serialize all information for array value types
        // 1. cast to ArrayValueType
//        auto arrayValueType = std::dynamic_pointer_cast<ArrayValue>(valueType);
//        // 2. create array value details
//        //        auto serializedArrayValue = SerializableDataValue_ArrayValue();
//        // 3. copy array values
//        for (const auto& value : arrayValueType->getValues()) {
//            //            serializedArrayValue.add_values(value);
//        }
        // 4. serialize array type
        //        serializeDataType(arrayValueType->getType(), serializedArrayValue.mutable_type());
        //        serializedDataValue->mutable_value()->PackFrom(serializedArrayValue);
    } else {
        // serialize all information for basic value types
        // 1. cast to BasicValueType
        auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        auto valueType = basicValueType->getType();

        if (valueType->isUndefined()) {
            //TODO: Not sure how to solve this
        } else if (valueType->isInteger()) {
            return context.int_val(std::stoi(basicValueType->getValue()));
        } else if (valueType->isFloat()) {
            return context.fpa_val(std::stod(basicValueType->getValue()));
        } else if (valueType->isBoolean()) {
            bool val = (strcasecmp(basicValueType->getValue().c_str(), "true") == 0 || std::atoi(basicValueType->getValue().c_str()) != 0);
            return context.bool_val(val);
        } else if (valueType->isChar() || valueType->isFixedChar()) {
            return context.string_val(basicValueType->getValue());
        } else {
            NES_FATAL_ERROR("");
        }

        // 2. create basic value details
        //        auto serializedBasicValue = SerializableDataValue_BasicValue();
        // 3. copy value
        //                serializedBasicValue.set_value(basicValueType->getValue());
        //                serializeDataType(basicValueType->getType(), serializedBasicValue.mutable_type());
        // 4. serialize basic type
        //        serializedDataValue->mutable_value()->PackFrom(serializedBasicValue);
    }
    NES_THROW_RUNTIME_ERROR("DataTypeSerializationUtil: serialization is not possible for " + valueType->toString());
}
}// namespace NES
