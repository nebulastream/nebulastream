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
#include <ostream>
#include <string>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

namespace NES
{

LogicalMapOperator::LogicalMapOperator(const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), mapFunction(mapFunction)
{
}

std::shared_ptr<NodeFunctionFieldAssignment> LogicalMapOperator::getMapFunction() const
{
    return mapFunction;
}

bool LogicalMapOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalMapOperator>(rhs)->getId() == id;
}

bool LogicalMapOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalMapOperator>(rhs))
    {
        const auto mapOperator = NES::Util::as<LogicalMapOperator>(rhs);
        return mapFunction->equal(mapOperator->mapFunction);
    }
    return false;
};

bool LogicalMapOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }

    /// use the default input schema to calculate the out schema of this operator.
    mapFunction->inferStamp(getInputSchema());

    const auto assignedField = mapFunction->getField();
    if (std::string fieldName = assignedField->getFieldName(); outputSchema.getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
        outputSchema.replaceTypeOfField(fieldName, assignedField->getStamp());
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
        outputSchema.addField(fieldName, assignedField->getStamp());
    }
    return true;
}

std::ostream& LogicalMapOperator::toDebugString(std::ostream& os) const
{
    return os << fmt::format("MAP(opId: {}, predicate: {})", id, *mapFunction);
}

std::ostream& LogicalMapOperator::toQueryPlanString(std::ostream& os) const
{
    return os << fmt::format("MAP({:q})", *mapFunction);
}

std::shared_ptr<Operator> LogicalMapOperator::copy()
{
    auto copy = std::make_shared<LogicalMapOperator>(Util::as<NodeFunctionFieldAssignment>(mapFunction->deepCopy()), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalMapOperator::inferStringSignature()
{
    NES_TRACE("LogicalMapOperator: Inferring String signature for {}", *this);
    INVARIANT(children.size() == 1, "Map should have 1 child, but got: {}", children.size());
    ///Infer query signatures for child operator
    const auto child = NES::Util::as<LogicalOperator>(children[0]);
    child->inferStringSignature();
    /// Infer signature for this operator.
    std::stringstream signatureStream;
    const auto childSignature = child->getHashBasedSignature();
    signatureStream << "MAP(" << *mapFunction << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
