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

#include "RustAnswerLogicalFunction.hpp"

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

RustAnswerLogicalFunction::RustAnswerLogicalFunction()
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT64))
{
}

DataType RustAnswerLogicalFunction::getDataType() const { return dataType; }

RustAnswerLogicalFunction RustAnswerLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction RustAnswerLogicalFunction::withInferredDataType(const Schema&) const
{
    // Always INT64, nothing to infer.
    return *this;
}

std::vector<LogicalFunction> RustAnswerLogicalFunction::getChildren() const { return {}; }

RustAnswerLogicalFunction RustAnswerLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const { return *this; }

std::string_view RustAnswerLogicalFunction::getType() const { return NAME; }

bool RustAnswerLogicalFunction::operator==(const RustAnswerLogicalFunction&) const { return true; }

std::string RustAnswerLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return "RustAnswerLogicalFunction(42 : INT64)";
    }
    return "rust_answer()";
}

Reflected Reflector<RustAnswerLogicalFunction>::operator()(const RustAnswerLogicalFunction&) const
{
    return reflect(std::monostate{});
}

RustAnswerLogicalFunction Unreflector<RustAnswerLogicalFunction>::operator()(const Reflected&) const
{
    return RustAnswerLogicalFunction{};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterRustAnswerLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<RustAnswerLogicalFunction>(arguments.reflected);
    }
    return RustAnswerLogicalFunction{};
}

}
