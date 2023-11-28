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
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/UDFLogicalOperator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>

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

bool UDFLogicalOperator::inferSchema() {
    // Set the input schema.
    if (!LogicalUnaryOperatorNode::inferSchema()) {
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
    verifySchemaCompatibility(udfDescriptor->getInputSchema(), children[0]->as<OperatorNode>()->getOutputSchema());
    return true;
}

Catalogs::UDF::UDFDescriptorPtr UDFLogicalOperator::getUDFDescriptor() const { return udfDescriptor; }

bool UDFLogicalOperator::equal(const NodePtr& other) const {
    return other->instanceOf<UDFLogicalOperator>() && *udfDescriptor == *other->as<UDFLogicalOperator>()->udfDescriptor;
}

bool UDFLogicalOperator::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<UDFLogicalOperator>()->id;
}

void UDFLogicalOperator::verifySchemaCompatibility(const Schema& udfInputSchema, const Schema& childOperatorOutputSchema) const {
    // The code below detects all schema violations, prints them to the ERROR output log,
    // then throws an exception containing all of them.
    // This makes it easier to users to fix all violations at once.
    std::vector<std::string> errors;
    if (udfInputSchema.getSize() != childOperatorOutputSchema.getSize()) {
        errors.push_back("UDF input schema and child operator output schema have different sizes.");
    }
    for (const auto& field : udfDescriptor->getInputSchema()->fields) {
        const auto& fieldName = field->getName();
        auto fieldInChild = childOperatorOutputSchema.getField(fieldName);
        if (!fieldInChild) {
            // If both schemas have size 1, the names do not matter.
            // In this case the UDF is assumed to not have a complex type but a boxed simple type, e.g., Integer, which is not named.
            // The UDF execution code just reads the first attribute from the child operator output schema, regardless of its name.
            if (udfInputSchema.getSize() == 1) {
                fieldInChild = childOperatorOutputSchema.fields[0];
            } else {
                errors.push_back(fmt::format("Could not find field in child operator output schema: {}", fieldName));
                continue;
            }
        }
        const auto type = field->getDataType();
        const auto childType = fieldInChild->getDataType();
        if (type->equals(DataTypeFactory::createInt64()) && childType->equals(DataTypeFactory::createUInt64())) {
            // This is not an error condition because we need to map timestamps, which are always UINT64, to Java long.
            NES_WARNING("Mapping UINT64 field in child operator output schema to signed Java long in UDF input schema: {}",
                        fieldName)
        } else if ((type->equals(DataTypeFactory::createInt8()) && childType->equals(DataTypeFactory::createUInt8()))
            || (type->equals(DataTypeFactory::createInt16()) && childType->equals(DataTypeFactory::createUInt16()))
            || (type->equals(DataTypeFactory::createInt32()) && childType->equals(DataTypeFactory::createUInt32()))) {
            errors.push_back(fmt::format(
                "Field data type is unsigned integer in child operator output schema; UDFs only support signed integers: {}",
                fieldName));
        } else if (!type->equals(childType)) {
            errors.push_back(fmt::format("Field data type differs in UDF input schema and child operator output schema: "
                                         "fieldName={}, udfType={}, parentType={}",
                                         fieldName,
                                         type->toString(),
                                         childType->toString()));
        }
    }
    if (!errors.empty()) {
        for (auto& error : errors) {
            NES_ERROR("{}", error);
        }
        std::stringstream message;
        message << "UDF input schema does not match child operator output schema:";
        if (errors.size() == 1) {
            message << " " << errors[0];
        } else {
            std::for_each(std::begin(errors), std::end(errors), [&message](const std::string& error) {
                message << "\n- " << error;
            });
        }
        throw TypeInferenceException(message.str());
    }
}
}// namespace NES