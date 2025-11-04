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

#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

SubLogicalFunction::SubLogicalFunction(const LogicalFunction& left, const LogicalFunction& right) : left(left), right(right) { };

bool SubLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const SubLogicalFunction*>(&rhs))
    {
        return left == other->left and right == other->right;
    }
    return false;
}

std::string SubLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SubLogicalFunction({} - {} : {})", left.explain(verbosity), right.explain(verbosity), dataType);
    }
    return fmt::format("{} - {}", left.explain(verbosity), right.explain(verbosity));
}

DataType SubLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction SubLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);
    copy.dataType = copy.left.getDataType().join(copy.right.getDataType()).value_or(DataType{DataType::Type::UNDEFINED});
    if (copy.dataType.isType(DataType::Type::UNDEFINED))
    {
        throw CannotInferStamp("Cannot apply subtraction to input function left: {}, right: {}", copy.left, copy.right);
    }
    return copy;
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
    copy.dataType = children[0].getDataType().join(children[1].getDataType()).value_or(DataType{DataType::Type::UNDEFINED});
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
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return SubLogicalFunction(arguments.children[0], arguments.children[1]).withInferredDataType(arguments.schema);
}

}
