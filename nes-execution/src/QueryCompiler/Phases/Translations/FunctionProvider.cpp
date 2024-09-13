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
#include <Execution/Functions/ReadFieldFunction.hpp>
#include <Execution/Functions/WriteFieldFunction.hpp>
#include <Execution/Functions/ConstantValueFunction.hpp>
#include <Execution/Functions/Registry/RegistryFunctionExecutable.hpp>
#include <Functions/FunctionNode.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <ErrorHandling.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/ConstantValueFunctionNode.hpp>

namespace NES::QueryCompilation
{
using namespace Runtime::Execution::Functions;

FunctionPtr FunctionProvider::lowerFunction(const FunctionNodePtr& functionNode)
{
    NES_INFO("Lower Function {}", functionNode->toString())
    if (auto constantValue = functionNode->as_if<ConstantValueFunctionNode>())
    {
        subFunctions.emplace_back(lowerFunction(child->as<FunctionNode>()));
    }

    /// The field assignment, field access and constant value nodes are special as they require a different treatment, due to them not simply getting a subFunction as a parameter.
    if (functionNode->instanceOf<FieldAssignmentFunctionNode>())
    {
        const auto fieldAssignmentNode = functionNode->as<FieldAssignmentFunctionNode>();
        const auto field = fieldAssignmentNode->getField()->getFieldName();
        return std::make_unique<WriteFieldFunction>(field, std::move(subFunctions[0]));
    }
    if (auto fieldWrite = functionNode->as_if<FieldAssignmentFunctionNode>())
    {
        return std::make_shared<WriteFieldFunction>(fieldWrite->getField()->getFieldName(), lowerFunction(fieldWrite->getAssignment()));
    }

    /// Calling the registry to create an executable function.
    auto function = Execution::Functions::RegistryFunctionExecutable::instance().tryCreate(functionNode->getType(), std::move(subFunctions));
    if (not function.has_value())
    {
        throw UnknownFunctionType(fmt::format("Can not lower function: {}", nodeFunction->getType()));
    }

    return std::move(function.value());
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
