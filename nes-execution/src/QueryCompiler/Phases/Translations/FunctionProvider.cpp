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

#include <cstdint>
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
#include <Util/Strings.hpp>
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
    const auto dataType = constantFunction->getStamp();
    switch (dataType.type)
    {
        case DataType::Type::UINT8: {
            if (auto intValue = NES::Util::from_chars<uint8_t>(stringValue))
            {
                return std::make_unique<ConstantUInt8ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to uint8", stringValue);
        };
        case DataType::Type::UINT16: {
            if (auto intValue = NES::Util::from_chars<uint16_t>(stringValue))
            {
                return std::make_unique<ConstantUInt16ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to uint16", stringValue);
        };
        case DataType::Type::UINT32: {
            if (auto intValue = NES::Util::from_chars<uint32_t>(stringValue))
            {
                return std::make_unique<ConstantUInt32ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to uint32", stringValue);
        };
        case DataType::Type::UINT64: {
            if (auto intValue = NES::Util::from_chars<uint64_t>(stringValue))
            {
                return std::make_unique<ConstantUInt64ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to uint64", stringValue);
        };
        case DataType::Type::INT8: {
            if (auto intValue = NES::Util::from_chars<int8_t>(stringValue))
            {
                return std::make_unique<ConstantInt8ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to int8", stringValue);
        };
        case DataType::Type::INT16: {
            if (auto intValue = NES::Util::from_chars<int16_t>(stringValue))
            {
                return std::make_unique<ConstantInt16ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to int16", stringValue);
        };
        case DataType::Type::INT32: {
            if (auto intValue = NES::Util::from_chars<int32_t>(stringValue))
            {
                return std::make_unique<ConstantInt32ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to int32", stringValue);
        };
        case DataType::Type::INT64: {
            if (auto intValue = NES::Util::from_chars<int64_t>(stringValue))
            {
                return std::make_unique<ConstantInt64ValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to int64", stringValue);
        };
        case DataType::Type::FLOAT32: {
            if (auto intValue = NES::Util::from_chars<float>(stringValue))
            {
                return std::make_unique<ConstantFloatValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to float", stringValue);
        };
        case DataType::Type::FLOAT64: {
            if (auto intValue = NES::Util::from_chars<double>(stringValue))
            {
                return std::make_unique<ConstantDoubleValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to double", stringValue);
        };
        case DataType::Type::CHAR:
            break;
        case DataType::Type::BOOLEAN: {
            if (auto intValue = NES::Util::from_chars<bool>(stringValue))
            {
                return std::make_unique<ConstantBooleanValueFunction>(intValue.value());
            }
            throw CannotFormatMalformedStringValue("Could not cast {} to bool", stringValue);
        }
        case DataType::Type::VARSIZED: {
            return std::make_unique<ExecutableFunctionConstantValueVariableSize>(
                reinterpret_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    throw UnknownPhysicalType("the basic type {} is not supported", dataType);
}
}
