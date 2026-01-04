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

#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>
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

PowLogicalFunction::PowLogicalFunction(const LogicalFunction& left, const LogicalFunction& right) : left(left), right(right) { };

bool PowLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const PowLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string PowLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("POW({}, {})", left.explain(verbosity), right.explain(verbosity));
}

DataType PowLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction PowLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);
    if ((!copy.left.getDataType().isNumeric()) || (!copy.left.getDataType().isNumeric()))
    {
        throw CannotInferStamp("Can only apply pow to two numeric input function, but got left: {}, right: {}", copy.left, copy.right);
    }
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    return copy;
};

std::vector<LogicalFunction> PowLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction PowLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    if ((!children.at(0).getDataType().isNumeric()) || (!children.at(1).getDataType().isNumeric()))
    {
        throw CannotInferStamp("Can only apply pow to two numeric input function, but got left: {}, right: {}", left, right);
    }
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    return copy;
};

std::string_view PowLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction PowLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterPowLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return PowLogicalFunction(arguments.children[0], arguments.children[1]).withInferredDataType(arguments.schema);
}

}
