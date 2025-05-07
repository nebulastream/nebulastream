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
#include <LogicalFunctions/ArithmeticalFunctions/SqrtFunction.hpp>

namespace NES::Logical
{

SqrtFunction::SqrtFunction(Function child) : stamp(child.getStamp()), child(child) {};

SqrtFunction::SqrtFunction(const SqrtFunction& other) : stamp(other.stamp), child(other.child)
{
}

bool SqrtFunction::operator==(const FunctionConcept& rhs) const
{
    auto other = dynamic_cast<const SqrtFunction*>(&rhs);
    if (other)
    {
        return child == other->child;
    }
    return false;
}

std::string SqrtFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SqrtFunction({} : {})",
            child.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("SQRT({})", child.explain(verbosity));
}

std::shared_ptr<DataType> SqrtFunction::getStamp() const
{
    return stamp;
};

Function SqrtFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function SqrtFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> SqrtFunction::getChildren() const
{
    return {child};
};

Function SqrtFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 1, "SqrtFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view SqrtFunction::getType() const
{
    return NAME;
}

SerializableFunction SqrtFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterSqrtFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "SqrtFunction requires exactly one child, but got {}", arguments.children.size());
    return SqrtFunction(arguments.children[0]);
}

}
