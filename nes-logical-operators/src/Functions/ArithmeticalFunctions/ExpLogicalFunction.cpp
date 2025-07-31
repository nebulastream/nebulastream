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

#include <Functions/ArithmeticalFunctions/ExpLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/ExpLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include "DataTypes/DataTypeProvider.hpp"

namespace NES
{

ExpLogicalFunction::ExpLogicalFunction(const LogicalFunction& child) : child(child) { };

bool ExpLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const ExpLogicalFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

DataType ExpLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction ExpLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    const std::string eulerNumber = "2.7182818284590452353602874713527";
    const ConstantValueLogicalFunction expConstantValue{DataTypeProvider::provideDataType(DataType::Type::FLOAT64), eulerNumber};
    return PowLogicalFunction(expConstantValue, newChild);
};

std::vector<LogicalFunction> ExpLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction ExpLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "ExpLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view ExpLogicalFunction::getType() const
{
    return NAME;
}

std::string ExpLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ExpLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("EXP({})", child.explain(verbosity));
}

SerializableFunction ExpLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterExpLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Function requires exactly one child, but got {}", arguments.children.size());
    }
    return ExpLogicalFunction(arguments.children[0]).withInferredDataType(arguments.schema);
}
}
