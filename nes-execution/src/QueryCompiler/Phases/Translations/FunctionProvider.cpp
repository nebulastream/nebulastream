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
#include <utility>
#include <vector>

#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
#include <Execution/Functions/ExecutableFunctionConstantValueVariableSize.hpp>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Functions/Function.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ExecutableFunctionRegistry.hpp>

namespace NES::QueryCompilation
{
using namespace Runtime::Execution::Functions;

std::unique_ptr<Function> FunctionProvider::lowerFunction(const std::shared_ptr<NodeFunction>& nodeFunction)
{
    /// 1. Check if the function is valid.
    auto ret = nodeFunction->validateBeforeLowering();
    INVARIANT(ret, "Function not valid: {}", *nodeFunction);

    /// 2. Recursively lower the children of the function node.
    std::vector<std::unique_ptr<Function>> childFunction;
    for (const auto& child : nodeFunction->getChildren())
    {
        childFunction.emplace_back(lowerFunction(NES::Util::as<NodeFunction>(child)));
    }

    /// 3. The field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a childFunction as a parameter.
    if (const auto fieldAccessNode = NES::Util::as_if<NodeFunctionFieldAccess>(nodeFunction); fieldAccessNode != nullptr)
    {
        return std::make_unique<ExecutableFunctionReadField>(fieldAccessNode->getFieldName());
    }
    if (const auto constantValueNode = NES::Util::as_if<NodeFunctionConstantValue>(nodeFunction); constantValueNode != nullptr)
    {
        return lowerConstantFunction(constantValueNode);
    }

    /// 4. Calling the registry to create an executable function.
    auto executableFunctionArguments = ExecutableFunctionRegistryArguments(std::move(childFunction));
    auto function = ExecutableFunctionRegistry::instance().create(nodeFunction->getType(), std::move(executableFunctionArguments));
    if (not function.has_value())
    {
        throw UnknownFunctionType(fmt::format("Can not lower function: {}", nodeFunction->getType()));
    }

    return std::move(function.value());
}

std::unique_ptr<Function> FunctionProvider::lowerConstantFunction(const std::shared_ptr<NodeFunctionConstantValue>& constantFunction)
{
    const auto stringValue = constantFunction->getConstantValue();
    const auto physicalType = constantFunction->getStamp().physicalType;
    switch (physicalType.type)
    {
        case PhysicalType::Type::UINT8: {
            auto intValue = static_cast<uint8_t>(std::stoul(stringValue));
            return std::make_unique<ConstantUInt8ValueFunction>(intValue);
        };
        case PhysicalType::Type::UINT16: {
            auto intValue = static_cast<uint16_t>(std::stoul(stringValue));
            return std::make_unique<ConstantUInt16ValueFunction>(intValue);
        };
        case PhysicalType::Type::UINT32: {
            auto intValue = static_cast<uint32_t>(std::stoul(stringValue));
            return std::make_unique<ConstantUInt32ValueFunction>(intValue);
        };
        case PhysicalType::Type::UINT64: {
            auto intValue = static_cast<uint64_t>(std::stoull(stringValue));
            return std::make_unique<ConstantUInt64ValueFunction>(intValue);
        };
        case PhysicalType::Type::INT8: {
            auto intValue = static_cast<int8_t>(std::stoi(stringValue));
            return std::make_unique<ConstantInt8ValueFunction>(intValue);
        };
        case PhysicalType::Type::INT16: {
            auto intValue = static_cast<int16_t>(std::stoi(stringValue));
            return std::make_unique<ConstantInt16ValueFunction>(intValue);
        };
        case PhysicalType::Type::INT32: {
            auto intValue = static_cast<int32_t>(std::stoi(stringValue));
            return std::make_unique<ConstantInt32ValueFunction>(intValue);
        };
        case PhysicalType::Type::INT64: {
            auto intValue = static_cast<int64_t>(std::stol(stringValue));
            return std::make_unique<ConstantInt64ValueFunction>(intValue);
        };
        case PhysicalType::Type::FLOAT32: {
            auto floatValue = std::stof(stringValue);
            return std::make_unique<ConstantFloatValueFunction>(floatValue);
        };
        case PhysicalType::Type::FLOAT64: {
            auto doubleValue = std::stod(stringValue);
            return std::make_unique<ConstantDoubleValueFunction>(doubleValue);
        };
        case PhysicalType::Type::CHAR:
            break;
        case PhysicalType::Type::BOOLEAN: {
            auto boolValue = static_cast<bool>(std::stoi(stringValue)) == 1;
            return std::make_unique<ConstantBooleanValueFunction>(boolValue);
        }
        case PhysicalType::Type::VARSIZED: {
            return std::make_unique<ExecutableFunctionConstantValueVariableSize>(
                reinterpret_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case PhysicalType::Type::UNDEFINED: {
            throw UnknownPhysicalType(fmt::format("the UNKNOWN type is not supported"));
        };
    }
    throw UnknownPhysicalType(fmt::format("the basic type {} is not supported", physicalType));
}
}
