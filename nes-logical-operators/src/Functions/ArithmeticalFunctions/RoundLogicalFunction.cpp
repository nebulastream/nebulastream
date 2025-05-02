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
#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <fmt/format.h>

namespace NES
{

RoundLogicalFunction::RoundLogicalFunction(LogicalFunction child) : stamp(child.getStamp()), child(child) {};

RoundLogicalFunction::RoundLogicalFunction(const RoundLogicalFunction& other) : stamp(other.stamp), child(other.child)
{
}

bool RoundLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const RoundLogicalFunction*>(&rhs))
    {
        return child == other->getChildren()[0];
    }
    return false;
}

std::string RoundLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("RoundLogicalFunction({} : {})", 
            child.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("ROUND({})", child.explain(verbosity));
}

std::shared_ptr<DataType> RoundLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction RoundLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction RoundLogicalFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> RoundLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction RoundLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "RoundLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view RoundLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction RoundLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterRoundLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "RoundLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return RoundLogicalFunction(arguments.children[0]);
}

}
