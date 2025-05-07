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

#include <FunctionRegistry.hpp>
#include <memory>
#include <sstream>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <SerializableFunction.pb.h>
#include <fmt/format.h>
#include <LogicalFunctions/ArithmeticalFunctions/AbsoluteFunction.hpp>

namespace NES::Logical
{

AbsoluteFunction::AbsoluteFunction(Function child) : stamp(child.getStamp()), child(child)
{
}

AbsoluteFunction::AbsoluteFunction(const AbsoluteFunction& other) : stamp(other.stamp), child(other.child)
{
}

std::shared_ptr<DataType> AbsoluteFunction::getStamp() const
{
    return stamp;
};

Function AbsoluteFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function AbsoluteFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<Function> AbsoluteFunction::getChildren() const
{
    return {child};
};

Function AbsoluteFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 1, "AbsoluteFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view AbsoluteFunction::getType() const
{
    return NAME;
}

bool AbsoluteFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const AbsoluteFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::string AbsoluteFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("AbsoluteFunction({} : {})",
            child.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("ABS({})", child.explain(verbosity));
}

SerializableFunction AbsoluteFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterAbsoluteFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "AbsoluteFunction requires exactly one child, but got {}", arguments.children.size());
    return AbsoluteFunction(arguments.children[0]);
}

}