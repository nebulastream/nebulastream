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
#include <utility>
#include <vector>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionCase.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

/// TODO #391: Use std::from_chars to check value for data type validity
FunctionItem::FunctionItem(int8_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::INT8), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint8_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::UINT8), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int16_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::INT16), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint16_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::UINT16), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int32_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::INT32), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint32_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::UINT32), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int64_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::INT64), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint64_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::UINT64), std::to_string(value)))
{
}

FunctionItem::FunctionItem(float value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::FLOAT32), std::to_string(value)))
{
}

FunctionItem::FunctionItem(double value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(LogicalType::FLOAT64), std::to_string(value)))
{
}

FunctionItem::FunctionItem(bool value)
    : FunctionItem(NodeFunctionConstantValue::create(
          DataTypeProvider::provideDataType(LogicalType::BOOLEAN), std::to_string(static_cast<int>(value))))
{
}

FunctionItem::FunctionItem(std::shared_ptr<NodeFunction> exp) : function(std::move(exp))
{
}

FunctionItem FunctionItem::as(std::string newName)
{
    ///rename function node
    PRECONDITION(
        Util::instanceOf<NodeFunctionFieldAccess>(function),
        "Renaming is only allowed on field access attributes, got: {}",
        function->getType());
    auto fieldAccessFunction = Util::as<NodeFunctionFieldAccess>(function);
    return NodeFunctionFieldRename::create(fieldAccessFunction, std::move(newName));
}

std::shared_ptr<NodeFunctionFieldAssignment> FunctionItem::operator=(const FunctionItem& assignItem) const
{
    return operator=(assignItem.getNodeFunction());
}

std::shared_ptr<NodeFunctionFieldAssignment> FunctionItem::operator=(const std::shared_ptr<NodeFunction>& assignFunction) const
{
    PRECONDITION(
        Util::instanceOf<NodeFunctionFieldAccess>(function),
        "We can only assign something to a field access function, got: {}",
        function->getType());
    return NodeFunctionFieldAssignment::create(Util::as<NodeFunctionFieldAccess>(function), assignFunction);
}

FunctionItem Attribute(std::string fieldName)
{
    return {NodeFunctionFieldAccess::create(std::move(fieldName))};
}

FunctionItem Attribute(std::string fieldName, BasicType type)
{
    return {NodeFunctionFieldAccess::create(DataTypeProvider::provideBasicType(type), std::move(fieldName))};
}

std::shared_ptr<NodeFunction> WHEN(const std::shared_ptr<NodeFunction>& conditionExp, const std::shared_ptr<NodeFunction>& valueExp)
{
    return NodeFunctionWhen::create(std::move(conditionExp), std::move(valueExp));
}

std::shared_ptr<NodeFunction> WHEN(const FunctionItem& conditionExp, const std::shared_ptr<NodeFunction>& valueExp)
{
    return WHEN(conditionExp.getNodeFunction(), std::move(valueExp));
}

std::shared_ptr<NodeFunction> WHEN(const std::shared_ptr<NodeFunction>& conditionExp, const FunctionItem& valueExp)
{
    return WHEN(std::move(conditionExp), valueExp.getNodeFunction());
}

std::shared_ptr<NodeFunction> WHEN(const FunctionItem& conditionExp, const FunctionItem& valueExp)
{
    return WHEN(conditionExp.getNodeFunction(), valueExp.getNodeFunction());
}

std::shared_ptr<NodeFunction>
CASE(const std::vector<std::shared_ptr<NodeFunction>>& whenFunctions, const std::shared_ptr<NodeFunction>& valueExp)
{
    return NodeFunctionCase::create(std::move(whenFunctions), std::move(valueExp));
}

std::shared_ptr<NodeFunction> CASE(const std::vector<std::shared_ptr<NodeFunction>>& whenFunctions, const FunctionItem& valueExp)
{
    return CASE(std::move(whenFunctions), valueExp.getNodeFunction());
}

std::shared_ptr<NodeFunction> FunctionItem::getNodeFunction() const
{
    return function;
}

FunctionItem::operator std::shared_ptr<NodeFunction>()
{
    return function;
}

}
