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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

LogicalMapOperator::LogicalMapOperator(const NodeFunctionFieldAssignmentPtr& mapFunction, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), mapFunction(mapFunction)
{
}

NodeFunctionFieldAssignmentPtr LogicalMapOperator::getMapFunction() const
{
    return mapFunction;
}

bool LogicalMapOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalMapOperator>(rhs)->getId() == id;
}

bool LogicalMapOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalMapOperator>(rhs))
    {
        auto mapOperator = NES::Util::as<LogicalMapOperator>(rhs);
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
    if (std::string fieldName = assignedField->getFieldName(); outputSchema->getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
        outputSchema->replaceField(fieldName, assignedField->getStamp());
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
        outputSchema->addField(fieldName, assignedField->getStamp());
    }
    return true;
}

std::string LogicalMapOperator::toString() const
{
    std::stringstream ss;
    ss << "MAP(opId: " << id << ": predicate: " << *mapFunction << ")";
    return ss.str();
}

std::shared_ptr<Operator> LogicalMapOperator::copy()
{
    auto copy = std::make_shared<LogicalMapOperator>(Util::as<NodeFunctionFieldAssignment>(mapFunction->deepCopy()), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalMapOperator::inferStringSignature()
{
    NES_TRACE("LogicalMapOperator: Inferring String signature for {}", toString());
    INVARIANT(children.size() == 1, "Map should have 1 child, but got: {}", children.size());
    ///Infer query signatures for child operator
    auto child = NES::Util::as<LogicalOperator>(children[0]);
    child->inferStringSignature();
    /// Infer signature for this operator.
    std::stringstream signatureStream;
    auto childSignature = child->getHashBasedSignature();
    signatureStream << "MAP(" << *mapFunction << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
