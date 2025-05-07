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
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/ArithmeticalFunctions/RoundFunction.hpp>

namespace NES::Logical
{

RoundFunction::RoundFunction(Function child) : stamp(child.getStamp()), child(child) {};

RoundFunction::RoundFunction(const RoundFunction& other) : stamp(other.stamp), child(other.child)
{
}

bool RoundFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const RoundFunction*>(&rhs))
    {
        return child == other->getChildren()[0];
    }
    return false;
}

std::string RoundFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("RoundFunction({} : {})",
            child.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("ROUND({})", child.explain(verbosity));
}

std::shared_ptr<DataType> RoundFunction::getStamp() const
{
    return stamp;
};

Function RoundFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function RoundFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> RoundFunction::getChildren() const
{
    return {child};
};

Function RoundFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 1, "RoundFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view RoundFunction::getType() const
{
    return NAME;
}

SerializableFunction RoundFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterRoundFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "RoundFunction requires exactly one child, but got {}", arguments.children.size());
    return RoundFunction(arguments.children[0]);
}

}
