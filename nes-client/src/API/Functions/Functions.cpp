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

#include <utility>
#include <vector>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <Functions/CaseFunctionNode.hpp>
#include <Functions/ConstantValueFunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
#include <Functions/WhenFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

FunctionItem::FunctionItem(int8_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint8_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int16_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint16_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int32_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint32_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt32(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(int64_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(uint64_t value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(float value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createFloat(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(double value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createDouble(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(bool value)
    : FunctionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createBoolean(), std::to_string(value)))
{
}

FunctionItem::FunctionItem(const char* value) : FunctionItem(DataTypeFactory::createFixedCharValue(value))
{
}

FunctionItem::FunctionItem(std::string const& value) : FunctionItem(DataTypeFactory::createFixedCharValue(value.c_str()))
{
}

FunctionItem::FunctionItem(ValueTypePtr value) : FunctionItem(ConstantValueFunctionNode::create(std::move(value)))
{
}

FunctionItem::FunctionItem(FunctionNodePtr exp) : function(std::move(exp))
{
}

FunctionItem FunctionItem::as(std::string newName)
{
    ///rename function node
    if (!function->instanceOf<FieldAccessFunctionNode>())
    {
        NES_ERROR("Renaming is only allowed on Field Access Attributes");
        NES_NOT_IMPLEMENTED();
    }
    auto fieldAccessFunction = function->as<FieldAccessFunctionNode>();
    return FieldRenameFunctionNode::create(fieldAccessFunction, std::move(newName));
}

FieldAssignmentFunctionNodePtr FunctionItem::operator=(FunctionItem assignItem)
{
    return operator=(assignItem.getFunctionNode());
}

FieldAssignmentFunctionNodePtr FunctionItem::operator=(FunctionNodePtr assignFunction)
{
    if (function->instanceOf<FieldAccessFunctionNode>())
    {
        return FieldAssignmentFunctionNode::create(function->as<FieldAccessFunctionNode>(), assignFunction);
    }
    NES_FATAL_ERROR("Function API: we can only assign something to a field access function");
    throw Exceptions::RuntimeException("Function API: we can only assign something to a field access function");
}

FunctionItem Attribute(std::string fieldName)
{
    return FunctionItem(FieldAccessFunctionNode::create(std::move(fieldName)));
}

FunctionItem Attribute(std::string fieldName, BasicType type)
{
    return FunctionItem(FieldAccessFunctionNode::create(DataTypeFactory::createType(type), std::move(fieldName)));
}

FunctionNodePtr WHEN(const FunctionNodePtr& conditionExp, const FunctionNodePtr& valueExp)
{
    return WhenFunctionNode::create(std::move(conditionExp), std::move(valueExp));
}

FunctionNodePtr WHEN(FunctionItem conditionExp, FunctionNodePtr valueExp)
{
    return WHEN(conditionExp.getFunctionNode(), std::move(valueExp));
}
FunctionNodePtr WHEN(FunctionNodePtr conditionExp, FunctionItem valueExp)
{
    return WHEN(std::move(conditionExp), valueExp.getFunctionNode());
}
FunctionNodePtr WHEN(FunctionItem conditionExp, FunctionItem valueExp)
{
    return WHEN(conditionExp.getFunctionNode(), valueExp.getFunctionNode());
}

FunctionNodePtr CASE(const std::vector<FunctionNodePtr>& whenFunctions, FunctionNodePtr valueExp)
{
    return CaseFunctionNode::create(std::move(whenFunctions), std::move(valueExp));
}
FunctionNodePtr CASE(std::vector<FunctionNodePtr> whenFunctions, FunctionItem valueExp)
{
    return CASE(std::move(whenFunctions), valueExp.getFunctionNode());
}

FunctionNodePtr FunctionItem::getFunctionNode() const
{
    return function;
}

FunctionItem::operator FunctionNodePtr()
{
    return function;
}

} /// namespace NES
