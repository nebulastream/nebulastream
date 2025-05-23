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

#include <Functions/ArithmeticalFunctions/CeilLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

CeilLogicalFunction::CeilLogicalFunction(LogicalFunction child) : dataType(child.getDataType()), child(child) { };

CeilLogicalFunction::CeilLogicalFunction(const CeilLogicalFunction& other) : dataType(other.dataType), child(other.child)
{
}

std::shared_ptr<DataType> CeilLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction CeilLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction CeilLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> CeilLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction CeilLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CeilLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view CeilLogicalFunction::getType() const
{
    return NAME;
}


bool CeilLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const CeilLogicalFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::string CeilLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("CeilLogicalFunction({} : {})", child.explain(verbosity), dataType->toString());
    }
    return fmt::format("CEIL({})", child.explain(verbosity));
}

SerializableFunction CeilLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterCeilLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "CeilLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return CeilLogicalFunction(arguments.children[0]);
}

}
