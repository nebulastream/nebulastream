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

#include <Execution/Functions/ConstantValueFunction.hpp>
#include <Execution/Functions/ReadFieldFunction.hpp>
#include <Execution/Functions/WriteFieldFunction.hpp>
#include <Functions/ArithmeticalFunctions/AddFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/DivFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/MulFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/SubFunctionNode.hpp>
#include <Functions/ConstantValueFunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/Functions/FunctionFunctionNode.hpp>
#include <Functions/LogicalFunctions/AndFunctionNode.hpp>
#include <Functions/LogicalFunctions/EqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/GreaterEqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/GreaterFunctionNode.hpp>
#include <Functions/LogicalFunctions/LessEqualsFunctionNode.hpp>
#include <Functions/LogicalFunctions/LessFunctionNode.hpp>
#include <Functions/LogicalFunctions/NegateFunctionNode.hpp>
#include <Functions/LogicalFunctions/OrFunctionNode.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::QueryCompilation
{
using namespace Runtime::Execution::Functions;

std::shared_ptr<Function> FunctionProvider::lowerFunction(const FunctionNodePtr& functionNode)
{
    NES_INFO("Lower Function {}", functionNode->toString())
    if (auto constantValue = functionNode->as_if<ConstantValueFunctionNode>())
    {
        return lowerConstantFunction(constantValue);
    }
    if (auto fieldAccess = functionNode->as_if<FieldAccessFunctionNode>())
    {
        return std::make_shared<ReadFieldFunction>(fieldAccess->getFieldName());
    }
    if (auto fieldWrite = functionNode->as_if<FieldAssignmentFunctionNode>())
    {
        return std::make_shared<WriteFieldFunction>(fieldWrite->getField()->getFieldName(), lowerFunction(fieldWrite->getAssignment()));
    }

    throw UnknownFunctionType();
}

FunctionPtr FunctionProvider::lowerConstantFunction(const std::shared_ptr<ConstantValueFunctionNode>& constantFunction)
{
    auto value = constantFunction->getConstantValue();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(constantFunction->getStamp());
    if (physicalType->isBasicType())
    {
        auto stringValue = std::dynamic_pointer_cast<BasicValue>(value)->value;
        auto basicType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::UINT_8: {
                auto intValue = (uint8_t)std::stoul(stringValue);
                return std::make_shared<ConstantUInt8ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                auto intValue = (uint16_t)std::stoul(stringValue);
                return std::make_shared<ConstantUInt16ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                auto intValue = (uint32_t)std::stoul(stringValue);
                return std::make_shared<ConstantUInt32ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                auto intValue = (uint64_t)std::stoull(stringValue);
                return std::make_shared<ConstantUInt64ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_8: {
                auto intValue = (int8_t)std::stoi(stringValue);
                return std::make_shared<ConstantInt8ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_16: {
                auto intValue = (int16_t)std::stoi(stringValue);
                return std::make_shared<ConstantInt16ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_32: {
                auto intValue = (int32_t)std::stoi(stringValue);
                return std::make_shared<ConstantInt32ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_64: {
                auto intValue = (int64_t)std::stol(stringValue);
                return std::make_shared<ConstantInt64ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                auto floatValue = std::stof(stringValue);
                return std::make_shared<ConstantFloatValueFunction>(floatValue);
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                auto doubleValue = std::stod(stringValue);
                return std::make_shared<ConstantDoubleValueFunction>(doubleValue);
            };
            case BasicPhysicalType::NativeType::CHAR:
                break;
            case BasicPhysicalType::NativeType::BOOLEAN: {
                auto boolValue = (bool)std::stoi(stringValue) == 1;
                return std::make_shared<ConstantBooleanValueFunction>(boolValue);
            };
            default: {
                throw UnknownPhysicalType("the basic type is not supported");
            }
        }
    }
    throw UnknownPhysicalType("type must be a basic types");
}
} /// namespace NES::QueryCompilation
