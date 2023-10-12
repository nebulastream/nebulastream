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
#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Catalogs/UDF/UDFDescriptor.hpp>
#include <Catalogs/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/UDFLogicalOperator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

UDFLogicalOperator::UDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), udfDescriptor(udfDescriptor) {}

void UDFLogicalOperator::inferStringSignature() {
    NES_TRACE("UDFLogicalOperator: Inferring String signature for {}", toString());
    NES_ASSERT(children.size() == 1, "UDFLogicalOperator should have exactly 1 child.");
    // Infer query signatures for child operator.
    auto child = children[0]->as<LogicalOperatorNode>();
    child->inferStringSignature();

    auto signatureStream = udfDescriptor->generateInferStringSignature();
    signatureStream << "." << *child->getHashBasedSignature().begin()->second.begin();
    auto signature = signatureStream.str();
    hashBasedSignature[hashGenerator(signature)] = {signature};
}

bool UDFLogicalOperator::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    // Set the input schema.
    if (!LogicalUnaryOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    // The output schema of this operation is determined by the UDF.
    outputSchema->clear();
    outputSchema->copyFields(udfDescriptor->getOutputSchema());
    // Update output schema by changing the qualifier and corresponding attribute names
    const auto newQualifierName = inputSchema->getQualifierNameForSystemGeneratedFields() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (auto& field : outputSchema->fields) {
        //Extract field name without qualifier
        auto fieldName = field->getName();
        //Add new qualifier name to the field and update the field name
        field->setName(newQualifierName + fieldName);
    }
    // TODO #3481 Check if the UDF input schema corresponds to the operator input schema of the parent operator
    return true;
}

Catalogs::UDF::UDFDescriptorPtr UDFLogicalOperator::getUDFDescriptor() const { return udfDescriptor; }

bool UDFLogicalOperator::equal(const NodePtr& other) const {
    return other->instanceOf<UDFLogicalOperator>() && *udfDescriptor == *other->as<UDFLogicalOperator>()->udfDescriptor;
}

bool UDFLogicalOperator::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<UDFLogicalOperator>()->id;
}
}// namespace NES