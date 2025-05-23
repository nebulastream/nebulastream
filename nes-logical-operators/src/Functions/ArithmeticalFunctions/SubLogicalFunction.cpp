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
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

SubLogicalFunction::SubLogicalFunction(LogicalFunction left, LogicalFunction right)
    : dataType(left.getDataType()->join(*right.getDataType())), left(left), right(right) { };

SubLogicalFunction::SubLogicalFunction(const SubLogicalFunction& other) : dataType(other.dataType), left(other.left), right(other.right)
{
}

bool SubLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const SubLogicalFunction*>(&rhs))
    {
        return left == other->left and right == other->right;
    }
    return false;
}

std::string SubLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SubLogicalFunction({} - {} : {})", left.explain(verbosity), right.explain(verbosity), dataType->toString());
    }
    return fmt::format("{} - {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> SubLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction SubLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction SubLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> SubLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction SubLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "SubLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType = children[0].getDataType()->join(*children[1].getDataType());
    return copy;
};

std::string_view SubLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction SubLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterSubLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "SubLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return SubLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
