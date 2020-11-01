#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Optimizer/Utils/DataTypeToZ3Expr.hpp>
#include <Util/Logger.hpp>
#include <string.h>
#include <z3++.h>

namespace NES {

z3::expr NES::DataTypeToZ3Expr::createForField(std::string fieldName, DataTypePtr dataType, z3::context& context) {
    if (dataType->isUndefined()) {
        NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data type of undefined type.");
    } else if (dataType->isInteger()) {
        return context.int_const(fieldName.c_str());
    } else if (dataType->isFloat()) {
        return context.fpa_const<64>(fieldName.c_str());
    } else if (dataType->isBoolean()) {
        return context.bool_const(fieldName.c_str());
    } else if (dataType->isChar() || dataType->isFixedChar()) {
        return context.constant(fieldName.c_str(), context.string_sort());
    } else if (dataType->isArray()) {
        auto arrayType = DataType::as<Array>(dataType);
        NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data type of array type.");
    }
    NES_THROW_RUNTIME_ERROR("Creating Z3 expression is not possible for " + dataType->toString());
}

z3::expr DataTypeToZ3Expr::createForDataValue(ValueTypePtr valueType, z3::context& context) {
    if (valueType->isArrayValue()) {
        NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data value of array type.");
    } else {
        auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        auto valueType = basicValueType->getType();
        if (valueType->isUndefined()) {
            NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data value of undefined type.");
        } else if (valueType->isInteger()) {
            return context.int_val(std::stoi(basicValueType->getValue()));
        } else if (valueType->isFloat()) {
            return context.fpa_val(std::stod(basicValueType->getValue()));
        } else if (valueType->isBoolean()) {
            bool val = (strcasecmp(basicValueType->getValue().c_str(), "true") == 0 || std::atoi(basicValueType->getValue().c_str()) != 0);
            return context.bool_val(val);
        } else if (valueType->isChar() || valueType->isFixedChar()) {
            return context.string_val(basicValueType->getValue());
        }
    }
    NES_THROW_RUNTIME_ERROR("Creating Z3 expression is not possible for " + valueType->toString());
}
}// namespace NES
