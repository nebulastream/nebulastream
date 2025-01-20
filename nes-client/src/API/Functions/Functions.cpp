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

#include <string>
#include <utility>
#include <vector>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <Functions/NodeFunctionCase.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

FunctionItem::FunctionItem(int8_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint8_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createUInt8(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int16_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createInt16(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint16_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createUInt16(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int32_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createInt32(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint32_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createUInt32(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int64_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createInt64(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint64_t value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createUInt64(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(float value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createFloat(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(double value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createDouble(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(bool value)
    : FunctionItem(NodeFunctionConstantValue::create(DataTypeFactory::createBoolean(), std::to_string(static_cast<int>(value))))
{
}

FunctionItem::FunctionItem(NodeFunctionPtr exp) : function(std::move(exp))
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

NodeFunctionFieldAssignmentPtr FunctionItem::operator=(FunctionItem assignItem)
{
    return operator=(assignItem.getNodeFunction());
}

NodeFunctionFieldAssignmentPtr FunctionItem::operator=(NodeFunctionPtr assignFunction)
{
    PRECONDITION(
        Util::instanceOf<NodeFunctionFieldAccess>(function),
        "We can only assign something to a field access function, got: {}",
        function->getType());
    return NodeFunctionFieldAssignment::create(Util::as<NodeFunctionFieldAccess>(function), assignFunction);
}

FunctionItem Attribute(std::string fieldName)
{
    return FunctionItem(NodeFunctionFieldAccess::create(std::move(fieldName)));
}

FunctionItem Attribute(std::string fieldName, BasicType type)
{
    return FunctionItem(NodeFunctionFieldAccess::create(DataTypeFactory::createType(type), std::move(fieldName)));
}

NodeFunctionPtr WHEN(const NodeFunctionPtr& conditionExp, const NodeFunctionPtr& valueExp)
{
    return NodeFunctionWhen::create(std::move(conditionExp), std::move(valueExp));
}

NodeFunctionPtr WHEN(FunctionItem conditionExp, NodeFunctionPtr valueExp)
{
    return WHEN(conditionExp.getNodeFunction(), std::move(valueExp));
}
NodeFunctionPtr WHEN(NodeFunctionPtr conditionExp, FunctionItem valueExp)
{
    return WHEN(std::move(conditionExp), valueExp.getNodeFunction());
}
NodeFunctionPtr WHEN(FunctionItem conditionExp, FunctionItem valueExp)
{
    return WHEN(conditionExp.getNodeFunction(), valueExp.getNodeFunction());
}

NodeFunctionPtr CASE(const std::vector<NodeFunctionPtr>& whenFunctions, NodeFunctionPtr valueExp)
{
    return NodeFunctionCase::create(std::move(whenFunctions), std::move(valueExp));
}
NodeFunctionPtr CASE(std::vector<NodeFunctionPtr> whenFunctions, FunctionItem valueExp)
{
    return CASE(std::move(whenFunctions), valueExp.getNodeFunction());
}

NodeFunctionPtr FunctionItem::getNodeFunction() const
{
    return function;
}

FunctionItem::operator NodeFunctionPtr()
{
    return function;
}

}
