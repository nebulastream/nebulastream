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
#include <API/Schema.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/LogicalFunctions/OrFunction.hpp>

namespace NES::Logical {

OrFunction::OrFunction(const OrFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

OrFunction::OrFunction(Function left, Function right)
    : stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), left(left), right(right)
{
}

bool OrFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const OrFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string OrFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} OR {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> OrFunction::getStamp() const
{
    return stamp;
};

Function OrFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

std::vector<Function> OrFunction::getChildren() const
{
    return {left, right};
};

Function OrFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "OrFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view OrFunction::getType() const
{
    return NAME;
}

Function OrFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> children;
    /// delegate stamp inference of children
    for (auto& node : getChildren())
    {
        children.push_back(node.withInferredStamp(schema));
    }
    /// check if children stamp is correct
    INVARIANT(
        *left.getStamp().get() == Boolean(), "the stamp of left child must be boolean, but was: {}", left.getStamp().get()->toString());
    INVARIANT(
        *right.getStamp().get() == Boolean(), "the stamp of right child must be boolean, but was: {}", right.getStamp().get()->toString());
    return this->withChildren(children);
}

bool OrFunction::validateBeforeLowering() const
{
    return dynamic_cast<const Boolean*>(left.getStamp().get()) && dynamic_cast<const Boolean*>(right.getStamp().get());
}

SerializableFunction OrFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterOrFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "OrFunction requires exactly two children, but got {}", arguments.children.size());
    return OrFunction(arguments.children[0], arguments.children[1]);
}

}
