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
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

LessEqualsLogicalFunction::LessEqualsLogicalFunction(const LessEqualsLogicalFunction& other)
    : left(other.left), right(other.right), dataType(other.dataType)
{
}

LessEqualsLogicalFunction::LessEqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(std::move(left)))
    , right(std::move(std::move(right)))
    , dataType(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}


bool LessEqualsLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const LessEqualsLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string LessEqualsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} <= {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> LessEqualsLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction LessEqualsLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction LessEqualsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> LessEqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction LessEqualsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "LessEqualsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view LessEqualsLogicalFunction::getType() const
{
    return NAME;
}


SerializableFunction LessEqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterLessEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 2, "LessEqualsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return LessEqualsLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
