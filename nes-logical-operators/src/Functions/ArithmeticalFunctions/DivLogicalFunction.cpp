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
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
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

DivLogicalFunction::DivLogicalFunction(const LogicalFunction& left, LogicalFunction right)
    : dataType(left.getDataType()), left(left), right(std::move(std::move(right))) { };

DivLogicalFunction::DivLogicalFunction(const DivLogicalFunction& other) : dataType(other.dataType), left(other.left), right(other.right)
{
}

bool DivLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const DivLogicalFunction*>(&rhs))
    {
        return left == other->left and right == other->right;
    }
    return false;
}

std::shared_ptr<DataType> DivLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction DivLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction DivLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> DivLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction DivLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "DivLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType = children[0].getDataType()->join(*children[1].getDataType());
    return copy;
};

std::string_view DivLogicalFunction::getType() const
{
    return NAME;
}

std::string DivLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("DivLogicalFunction({} / {} : {})", left.explain(verbosity), right.explain(verbosity), dataType->toString());
    }
    return fmt::format("{} / {}", left.explain(verbosity), right.explain(verbosity));
}

SerializableFunction DivLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterDivLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "DivLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return DivLogicalFunction(arguments.children[0], arguments.children[1]);
}


}
