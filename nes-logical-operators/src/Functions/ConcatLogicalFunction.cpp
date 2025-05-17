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
#include <string>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

ConcatLogicalFunction::ConcatLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : dataType(left.getDataType()->join(*right.getDataType())), left(left), right(right)
{
}

ConcatLogicalFunction::ConcatLogicalFunction(const ConcatLogicalFunction& other)
    : dataType(other.dataType), left(other.left), right(other.right)
{
}

bool ConcatLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const ConcatLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string ConcatLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("CONCAT({}, {})", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> ConcatLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction ConcatLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ConcatLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> ConcatLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction ConcatLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType = children[0].getDataType()->join(*children[1].getDataType());
    return copy;
};

std::string_view ConcatLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction ConcatLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConcatLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 2, "ConcatLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return ConcatLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
