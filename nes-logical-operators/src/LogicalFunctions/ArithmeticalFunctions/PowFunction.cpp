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
#include <LogicalFunctions/ArithmeticalFunctions/PowFunction.hpp>

namespace NES::Logical
{

PowFunction::PowFunction(Function left, Function right)
    : stamp(left.getStamp()->join(*right.getStamp().get())), left(left), right(right) {};

PowFunction::PowFunction(const PowFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

bool PowFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const PowFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string PowFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("POW({}, {})", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> PowFunction::getStamp() const
{
    return stamp;
};

Function PowFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function PowFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> PowFunction::getChildren() const
{
    return {left, right};
};

Function PowFunction::withChildren(const std::vector<Function>& children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp()->join(*children[1].getStamp());
    return copy;
};

std::string_view PowFunction::getType() const
{
    return NAME;
}

SerializableFunction PowFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterPowFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "PowFunction requires exactly two children, but got {}", arguments.children.size());
    return PowFunction(arguments.children[0], arguments.children[1]);
}

}
