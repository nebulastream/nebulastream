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

#include <Operators/Expressions/BinaryExpressionNode.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Statistics/SynopsisProbeParameter/CountMinProbeParameter.hpp>
#include <Util/Logger/Logger.hpp>

#include <Common/ValueTypes/BasicValue.hpp>
//#include <Common/ValueTypes/ValueType.hpp>
#include <Common/DataTypes/DataType.hpp>
//#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>

namespace NES::Experimental::Statistics {

CountMinProbeParameter::CountMinProbeParameter(const NES::ExpressionNodePtr& expression) {

    if (!expression->instanceOf<BinaryExpressionNode>()) {
        NES_ERROR("Invalid expression used to define queried statistic")
    }

    auto children = expression->getChildren();
    if (children.size() != 2) {
        NES_ERROR("Invalid expression used to define queried statistic")
    }

    // extract the fieldName and the const value from the expression
    if (children[0]->instanceOf<ConstantValueExpressionNode>() && children[1]->instanceOf<FieldAccessExpressionNode>()) {
        auto valueType = children[0]->as<ConstantValueExpressionNode>()->getConstantValue();
        auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        auto valueTypeType = basicValueType->dataType;
        if (valueTypeType->isInteger()) {
            constQueryValue = static_cast<uint64_t>(std::stoi(basicValueType->value));
        }
        fieldName = children[1]->as<FieldAccessExpressionNode>()->getFieldName();
    } else if (children[0]->instanceOf<FieldAccessExpressionNode>() && children[1]->instanceOf<ConstantValueExpressionNode>()) {
        auto valueType = children[1]->as<ConstantValueExpressionNode>()->getConstantValue();
        auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
        auto valueTypeType = basicValueType->dataType;
        if (valueTypeType->isInteger()) {
            constQueryValue = static_cast<uint64_t>(std::stoi(basicValueType->value));
        }
        fieldName = children[0]->as<FieldAccessExpressionNode>()->getFieldName();
    } else {
        NES_ERROR("Error: either both or neither parts of the expression are a field")
    }

    if (expression->instanceOf<EqualsExpressionNode>()) {
        queryType = StatisticQueryType::POINT_QUERY;
        inverse = false;
    } else if (expression->instanceOf<GreaterEqualsExpressionNode>()) {
        queryType = StatisticQueryType::RANGE_QUERY;
        inverse = true;
    } else if (expression->instanceOf<GreaterExpressionNode>()) {
        queryType = StatisticQueryType::RANGE_QUERY;
        inverse = true;
        constQueryValue += 1;
    } else if (expression->instanceOf<LessEqualsExpressionNode>()) {
        queryType = StatisticQueryType::RANGE_QUERY;
        inverse = false;
        constQueryValue += 1;
    } else if (expression->instanceOf<LessExpressionNode>()) {
        queryType = StatisticQueryType::RANGE_QUERY;
        inverse = false;
    } else {
        NES_ERROR("Invalid expression used to define queried statistic")
    }
}

StatisticQueryType CountMinProbeParameter::getQueryType() const { return queryType; }

bool CountMinProbeParameter::isInverse() const { return inverse; }

const std::string& CountMinProbeParameter::getFieldName() const { return fieldName; }

uint64_t CountMinProbeParameter::getConstQueryValue() const { return constQueryValue; }

}// namespace NES::Experimental::Statistics
