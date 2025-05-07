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

#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/ArithmeticalFunctions/ExpFunction.hpp>

namespace NES::Logical
{

ExpFunction::ExpFunction(Function child) : stamp(child.getStamp()), child(child) {};

ExpFunction::ExpFunction(const ExpFunction& other) : child(other.getChildren()[0])
{
}

bool ExpFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const ExpFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::shared_ptr<DataType> ExpFunction::getStamp() const
{
    return stamp;
};

Function ExpFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function ExpFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<Function> ExpFunction::getChildren() const
{
    return {child};
};

Function ExpFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 1, "ExpFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view ExpFunction::getType() const
{
    return NAME;
}

std::string ExpFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ExpFunction({} : {})",
            child.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("EXP({})", child.explain(verbosity));
}

SerializableFunction ExpFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}


FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterExpFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "ExpFunction requires exactly one child, but got {}", arguments.children.size());
    return ExpFunction(arguments.children[0]);
}
}
