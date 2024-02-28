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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {

LogicalMapOperator::LogicalMapOperator(const FieldAssignmentExpressionNodePtr& mapExpression, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), mapExpression(mapExpression) {}

FieldAssignmentExpressionNodePtr LogicalMapOperator::getMapExpression() const { return mapExpression; }

bool LogicalMapOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalMapOperator>()->getId() == id;
}

bool LogicalMapOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalMapOperator>()) {
        auto mapOperator = rhs->as<LogicalMapOperator>();
        return mapExpression->equal(mapOperator->mapExpression);
    }
    return false;
};

bool LogicalMapOperator::inferSchema() {
    // infer the default input and output schema
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }

    // use the default input schema to calculate the out schema of this operator.
    mapExpression->inferStamp( getInputSchema());

    auto assignedField = mapExpression->getField();
    std::string fieldName = assignedField->getFieldName();

    if (outputSchema->getField(fieldName)) {
        // The assigned field is part of the current schema.
        // Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
        outputSchema->replaceField(fieldName, assignedField->getStamp());
    } else {
        // The assigned field is not part of the current schema.
        // Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
        outputSchema->addField(fieldName, assignedField->getStamp());
    }
    return true;
}

std::string LogicalMapOperator::toString() const {
    std::stringstream ss;
    ss << "MAP(" << id << ")";
    return ss.str();
}

OperatorPtr LogicalMapOperator::copy() {
    auto copy = LogicalOperatorFactory::createMapOperator(mapExpression, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalMapOperator::inferStringSignature() {
    NES_TRACE("LogicalMapOperator: Inferring String signature for {}", toString());
    NES_ASSERT(children.size() == 1, "LogicalMapOperator: Map should have 1 child.");
    //Infer query signatures for child operator
    auto child = children[0]->as<LogicalOperator>();
    child->inferStringSignature();
    // Infer signature for this operator.
    std::stringstream signatureStream;
    auto childSignature = child->getHashBasedSignature();
    signatureStream << "MAP(" + mapExpression->toString() + ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
