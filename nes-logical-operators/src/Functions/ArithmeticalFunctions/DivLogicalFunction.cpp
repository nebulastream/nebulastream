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

#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Schema/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

DivLogicalFunction::DivLogicalFunction(const LogicalFunction& left, LogicalFunction right)
    : left(left), right(std::move(right)) { };

bool DivLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const DivLogicalFunction*>(&rhs))
    {
        return left == other->left and right == other->right;
    }
    return false;
}

DataType DivLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction DivLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);
    if (!copy.left.getDataType().isNumeric() || !copy.right.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot apply division to non-numeric input function left: {}, right: {}", copy.left, copy.right);
    }
    copy.dataType = copy.left.getDataType();

    return copy;
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
    copy.dataType = children[0].getDataType().join(children[1].getDataType()).value_or(DataType{DataType::Type::UNDEFINED});
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
        return fmt::format("DivLogicalFunction({} / {} : {})", left.explain(verbosity), right.explain(verbosity), dataType);
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
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return DivLogicalFunction(arguments.children[0], arguments.children[1]).withInferredDataType(arguments.schema);
}


}
