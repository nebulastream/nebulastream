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
#include <LogicalFunctions/Function.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <sstream>
#include <LogicalFunctions/ConcatFunction.hpp>

namespace NES::Logical
{

ConcatFunction::ConcatFunction(Function left, Function right)
    : stamp(left.getStamp()->join(*right.getStamp())), left(left), right(right)
{
}

ConcatFunction::ConcatFunction(const ConcatFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

bool ConcatFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const ConcatFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string ConcatFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("CONCAT({}, {})", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> ConcatFunction::getStamp() const
{
    return stamp;
};

Function ConcatFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function ConcatFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> ConcatFunction::getChildren() const
{
    return {left, right};
};

Function ConcatFunction::withChildren(const std::vector<Function>& children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp()->join(*children[1].getStamp().get());
    return copy;
};

std::string_view ConcatFunction::getType() const
{
    return NAME;
}

SerializableFunction ConcatFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterConcatFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "ConcatFunction requires exactly two children, but got {}", arguments.children.size());
    return ConcatFunction(arguments.children[0], arguments.children[1]);
}

}
