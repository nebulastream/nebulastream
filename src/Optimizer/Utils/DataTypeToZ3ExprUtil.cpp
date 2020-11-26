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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Utils/DataTypeToZ3ExprUtil.hpp>
#include <Optimizer/Utils/ReturnValue.hpp>
#include <Util/Logger.hpp>
#include <string.h>
#include <z3++.h>

namespace NES::Optimizer {

ReturnValuePtr DataTypeToZ3ExprUtil::createForField(std::string fieldName, DataTypePtr dataType, z3::context& context) {
    z3::ExprPtr expr;
    if (dataType->isInteger()) {
        expr = std::make_shared<z3::expr>(context.int_const(fieldName.c_str()));
    } else if (dataType->isFloat()) {
        expr = std::make_shared<z3::expr>(context.fpa_const<64>(fieldName.c_str()));
    } else if (dataType->isBoolean()) {
        expr = std::make_shared<z3::expr>(context.bool_const(fieldName.c_str()));
    } else if (dataType->isChar() || dataType->isFixedChar()) {
        expr = std::make_shared<z3::expr>(context.constant(fieldName.c_str(), context.string_sort()));
    } else {
        NES_THROW_RUNTIME_ERROR("Creating Z3 expression is not possible for " + dataType->toString());
    }
    std::map<std::string, z3::ExprPtr> constMap{{fieldName, expr}};
    return ReturnValue::create(expr, constMap);
}

ReturnValuePtr DataTypeToZ3ExprUtil::createForDataValue(ValueTypePtr valueType, z3::context& context) {
    if (valueType->isArrayValue()) {
        NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data value of array type.");
    } else {
        auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        auto valueType = basicValueType->getType();
        z3::ExprPtr expr;
        if (valueType->isUndefined()) {
            NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data value of undefined type.");
        } else if (valueType->isInteger()) {
            expr = std::make_shared<z3::expr>(context.int_val(std::stoi(basicValueType->getValue())));
        } else if (valueType->isFloat()) {
            expr = std::make_shared<z3::expr>(context.fpa_val(std::stod(basicValueType->getValue())));
        } else if (valueType->isBoolean()) {
            bool val = (strcasecmp(basicValueType->getValue().c_str(), "true") == 0
                        || std::atoi(basicValueType->getValue().c_str()) != 0);
            expr = std::make_shared<z3::expr>(context.bool_val(val));
        } else if (valueType->isChar() || valueType->isFixedChar()) {
            expr = std::make_shared<z3::expr>(context.string_val(basicValueType->getValue()));
        } else {
            NES_THROW_RUNTIME_ERROR("Creating Z3 expression is not possible for " + valueType->toString());
        }
        return ReturnValue::create(expr, {});
    }
}
}// namespace NES::Optimizer
