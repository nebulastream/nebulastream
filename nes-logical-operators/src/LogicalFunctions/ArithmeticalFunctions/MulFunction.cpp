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
#include <sstream>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/ArithmeticalFunctions/MulFunction.hpp>

namespace NES::Logical
{
MulFunction::MulFunction(Function left, Function right)
    : stamp(left.getStamp()->join(*right.getStamp())), left(left), right(right)
{
}

MulFunction::MulFunction(const MulFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

bool MulFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const MulFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string MulFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("MulFunction({} * {} : {})",
            left.explain(verbosity),
            right.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("{} * {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> MulFunction::getStamp() const
{
    return stamp;
};

Function MulFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function MulFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> MulFunction::getChildren() const
{
    return {left, right};
};

Function MulFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "MulFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp()->join(*children[1].getStamp());
    return copy;
};

std::string_view MulFunction::getType() const
{
    return NAME;
}

SerializableFunction MulFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterMulFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "MulFunction requires exactly two children, but got {}", arguments.children.size());
    return MulFunction(arguments.children[0], arguments.children[1]);
}

}
