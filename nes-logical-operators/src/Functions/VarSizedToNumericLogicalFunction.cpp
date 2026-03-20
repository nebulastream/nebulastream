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

#include <Functions/VarSizedToNumericLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
    {

        VarSizedToNumericLogicalFunction::VarSizedToNumericLogicalFunction(const LogicalFunction& child, const DataType& targetType)
            : outputType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), targetType(targetType), child(child)
        {
        }

        DataType VarSizedToNumericLogicalFunction::getDataType() const
        {
            return outputType;
        }

        DataType VarSizedToNumericLogicalFunction::getTargetType() const
        {
            return targetType;
        }

        VarSizedToNumericLogicalFunction VarSizedToNumericLogicalFunction::withDataType(const DataType& newDataType) const
        {
            auto copy = *this;
            copy.outputType = newDataType;
            return copy;
        }

        bool VarSizedToNumericLogicalFunction::isSupportedNumericType(const DataType::Type type)
        {
            switch (type)
            {
                case DataType::Type::INT8:
                case DataType::Type::INT16:
                case DataType::Type::INT32:
                case DataType::Type::INT64:
                case DataType::Type::UINT8:
                case DataType::Type::UINT16:
                case DataType::Type::UINT32:
                case DataType::Type::UINT64:
                case DataType::Type::FLOAT32:
                case DataType::Type::FLOAT64:
                    return true;
                default:
                    return false;
            }
        }

        LogicalFunction VarSizedToNumericLogicalFunction::withInferredDataType(const Schema& schema) const
        {
            const auto newChildren = getChildren()
                | std::views::transform([&schema](auto& currentChild) { return currentChild.withInferredDataType(schema); })
                | std::ranges::to<std::vector>();

            INVARIANT(newChildren.size() == 1, "VarSizedToNumericLogicalFunction expects exactly one child function but has {}", newChildren.size());

            auto childDataType = newChildren[0].getDataType();
            if (childDataType.type != DataType::Type::UNDEFINED && childDataType.type != DataType::Type::VARSIZED)
            {
                throw DifferentFieldTypeExpected("VarSizedToNumeric expects a VARSIZED input, but got {}", childDataType);
            }

            if (!isSupportedNumericType(targetType.type))
            {
                throw DifferentFieldTypeExpected("VarSizedToNumeric expects a numeric target type, but got {}", targetType);
            }

            auto inferredOutputType = targetType;
            inferredOutputType.nullable = std::ranges::any_of(newChildren, [](const auto& currentChild) {
                                                                  return currentChild.getDataType().nullable;
                                                              });

            return withDataType(inferredOutputType).withChildren(newChildren);
        }

        std::vector<LogicalFunction> VarSizedToNumericLogicalFunction::getChildren() const
        {
            return {child};
        }

        VarSizedToNumericLogicalFunction VarSizedToNumericLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
        {
            PRECONDITION(children.size() == 1, "VarSizedToNumericLogicalFunction requires exactly one child, but got {}", children.size());
            auto copy = *this;
            copy.child = children[0];
            return copy;
        }

        std::string_view VarSizedToNumericLogicalFunction::getType() const
        {
            return NAME;
        }

        bool VarSizedToNumericLogicalFunction::operator==(const VarSizedToNumericLogicalFunction& rhs) const
        {
            return this->outputType == rhs.outputType && this->targetType == rhs.targetType && this->child == rhs.child;
        }

        std::string VarSizedToNumericLogicalFunction::explain(ExplainVerbosity verbosity) const
        {
            if (verbosity == ExplainVerbosity::Debug)
            {
                return fmt::format(
                    "VarSizedToNumericLogicalFunction({} -> {}, outputType={})",
                    child.explain(verbosity),
                    targetType,
                    outputType);
            }

            return fmt::format("VarSizedToNumeric({}, {})", child.explain(verbosity), targetType);
        }

        Reflected Reflector<VarSizedToNumericLogicalFunction>::operator()(const VarSizedToNumericLogicalFunction& function) const
        {
            return reflect(detail::ReflectedVarSizedToNumericLogicalFunction{
                .child = function.getChildren()[0],
                .targetType = function.getTargetType(),
            });
        }

        VarSizedToNumericLogicalFunction Unreflector<VarSizedToNumericLogicalFunction>::operator()(const Reflected& reflected) const
        {
            auto [childFunction, targetType] = unreflect<detail::ReflectedVarSizedToNumericLogicalFunction>(reflected);
            return VarSizedToNumericLogicalFunction(childFunction, targetType);
        }

        LogicalFunctionRegistryReturnType
        LogicalFunctionGeneratedRegistrar::RegisterVarSizedToNumericLogicalFunction(LogicalFunctionRegistryArguments arguments)
        {
            if (!arguments.reflected.isEmpty())
            {
                return unreflect<VarSizedToNumericLogicalFunction>(arguments.reflected);
            }

            throw CannotDeserialize(
                "VarSizedToNumericLogicalFunction requires a reflected target type argument. "
                "Creating it from children only is not supported.");
        }

    }