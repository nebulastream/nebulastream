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
#include <LogicalFunctions/ArithmeticalFunctions/ModuloFunction.hpp>

namespace NES::Logical
{

ModuloFunction::ModuloFunction(const ModuloFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

ModuloFunction::ModuloFunction(Function left, Function right)
    : stamp(left.getStamp()->join(*right.getStamp())), left(left), right(right)
{
}

bool ModuloFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const ModuloFunction*>(&rhs))
    {
        return left == other->left and right == other->right;
    }
    return false;
}

std::shared_ptr<DataType> ModuloFunction::getStamp() const
{
    return stamp;
};

Function ModuloFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function ModuloFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> ModuloFunction::getChildren() const
{
    return {left, right};
};

Function ModuloFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "ModuloFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp()->join(*children[1].getStamp());
    return copy;
};

std::string_view ModuloFunction::getType() const
{
    return NAME;
}

std::string ModuloFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ModuloFunction({} % {} : {})",
            left.explain(verbosity),
            right.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("{} % {}", left.explain(verbosity), right.explain(verbosity));
}

SerializableFunction ModuloFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterModuloFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "ModuloFunction requires exactly two children, but got {}", arguments.children.size());
    return ModuloFunction(arguments.children[0], arguments.children[1]);
}


}
