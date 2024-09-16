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

#include <vector>
#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Functions/ExecutableFunctionWriteField.hpp>
#include <Execution/Functions/Registry/RegistryFunctionExecutable.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::QueryCompilation
{
using namespace Runtime::Execution::Functions;

FunctionPtr FunctionProvider::lowerFunction(const NodeFunctionPtr& functionNode)
{
    /// 1. Check if the function is valid.
    if (not functionNode->validateBeforeLowering())
    {
        throw InvalidFunction();
    }

    /// 2. Recursively lower the children of the function node.
    const auto children = functionNode->getChildren();
    std::vector<FunctionPtr> subFunctions;
    for (const auto& child : children)
    {
        subFunctions.emplace_back(lowerFunction(child->as<FunctionNode>()));
    }

    /// 3. The field assignment, field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a subFunction as a parameter.
    if (const auto fieldAssignmentNode = functionNode->as_if<NodeFunctionFieldAssignment>())
    {
        return std::make_unique<ExecutableFunctionWriteField>(fieldAssignmentNode->getField()->getFieldName(), std::move(subFunctions[0]));
    }
    if (const auto fieldAccessNode = functionNode->as_if<NodeFunctionFieldAccess>())
    {
        return std::make_unique<ExecutableFunctionReadField>(fieldAccessNode->getFieldName());
    }
    if (const auto constantValueNode = functionNode->as_if<NodeFunctionConstantValue>())
    {
        return lowerConstantFunction(constantValueNode);
    }

    /// 4. Calling the registry to create an executable function.
    auto function
        = Execution::Functions::RegistryFunctionExecutable::instance().tryCreate(functionNode->getName(), std::move(subFunctions));
    if (not function.has_value())
    {
        throw UnknownFunctionType();
    }

    return std::move(function.value());
}

FunctionPtr FunctionProvider::lowerConstantFunction(const std::shared_ptr<NodeFunctionConstantValue>& constantFunction)
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
                auto intValue = static_cast<uint8_t>(std::stoul(stringValue));
                return std::make_unique<ConstantUInt8ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                auto intValue = static_cast<uint16_t>(std::stoul(stringValue));
                return std::make_unique<ConstantUInt16ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                auto intValue = static_cast<uint32_t>(std::stoul(stringValue));
                return std::make_unique<ConstantUInt32ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                auto intValue = static_cast<uint64_t>(std::stoull(stringValue));
                return std::make_unique<ConstantUInt64ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_8: {
                auto intValue = static_cast<int8_t>(std::stoi(stringValue));
                return std::make_unique<ConstantInt8ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_16: {
                auto intValue = static_cast<int16_t>(std::stoi(stringValue));
                return std::make_unique<ConstantInt16ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_32: {
                auto intValue = static_cast<int32_t>(std::stoi(stringValue));
                return std::make_unique<ConstantInt32ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_64: {
                auto intValue = static_cast<int64_t>(std::stol(stringValue));
                return std::make_unique<ConstantInt64ValueFunction>(intValue);
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                auto floatValue = std::stof(stringValue);
                return std::make_unique<ConstantFloatValueFunction>(floatValue);
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                auto doubleValue = std::stod(stringValue);
                return std::make_unique<ConstantDoubleValueFunction>(doubleValue);
            };
            case BasicPhysicalType::NativeType::CHAR:
                break;
            case BasicPhysicalType::NativeType::BOOLEAN: {
                auto boolValue = static_cast<bool>(std::stoi(stringValue)) == 1;
                return std::make_unique<ConstantBooleanValueFunction>(boolValue);
            };
            default: {
                throw UnknownPhysicalType("the basic type is not supported");
            }
        }
    }
    throw UnknownPhysicalType("type must be a basic types");
}
}
