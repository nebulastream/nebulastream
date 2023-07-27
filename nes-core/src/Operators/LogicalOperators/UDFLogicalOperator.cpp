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
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/LogicalOperators/UDFLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <numeric>

namespace NES {

UDFLogicalOperator::UDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), udfDescriptor(udfDescriptor) {}

void UDFLogicalOperator::inferStringSignature() {
    NES_TRACE("UDFLogicalOperator: Inferring String signature for {}", toString());
    NES_ASSERT(children.size() == 1, "UDFLogicalOperator should have exactly 1 child.");
    // Infer query signatures for child operator.
    auto child = children[0]->as<LogicalOperatorNode>();
    child->inferStringSignature();

    auto signatureStream = std::stringstream{};

    // IF JAVA UDF
    if (udfDescriptor->instanceOf<Catalogs::UDF::JavaUDFDescriptorPtr>()) {
        // Infer signature for this operator based on the UDF metadata (class name and UDF method), the serialized instance,
        // and the byte code list. We can ignore the schema information because it is determined by the UDF method signature.
        auto javaUDFDescriptor = getUDFDescriptor()->as<Catalogs::UDF::JavaUDFDescriptor>(getUDFDescriptor());
        auto elementHash = std::hash<jni::JavaSerializedInstance::value_type>{};

        // Hash the contents of a byte array (i.e., the serialized instance and the byte code of a class)
        // based on the hashes of the individual elements.
        auto charArrayHashHelper = [&elementHash](std::size_t h, char v) {
            return h = h * 31 + elementHash(v);
        };

        // Compute hashed value of the UDF instance.
        auto& instance = javaUDFDescriptor->getSerializedInstance();
        auto instanceHash = std::accumulate(instance.begin(), instance.end(), instance.size(), charArrayHashHelper);

        // Compute hashed value of the UDF byte code list.
        auto stringHash = std::hash<std::string>{};
        auto& byteCodeList = javaUDFDescriptor->getByteCodeList();
        auto byteCodeListHash = std::accumulate(
            byteCodeList.begin(),
            byteCodeList.end(),
            byteCodeList.size(),
            [&stringHash, &charArrayHashHelper](std::size_t h, jni::JavaUDFByteCodeList::value_type v) {
                /* It is not possible to hash unordered_map directly in C++, this will be
                                     * investigated in issue #3584
                                     */

                auto& className = v.first;
                h = h * 31 + stringHash(className);
                auto& byteCode = v.second;
                h = h * 31 + std::accumulate(byteCode.begin(), byteCode.end(), byteCode.size(), charArrayHashHelper);
                return h;
            });

        signatureStream << "JAVA_UDF(" << javaUDFDescriptor->getClassName() << "." << javaUDFDescriptor->getMethodName()
                        << ", instance=" << instanceHash << ", byteCode=" << byteCodeListHash << ")"
                        << "." << *child->getHashBasedSignature().begin()->second.begin();
        auto signature = signatureStream.str();
        hashBasedSignature[stringHash(signature)] = {signature};
    } else if (udfDescriptor->instanceOf<Catalogs::UDF::PythonUDFDescriptorPtr>()) {
        // IF PYTHON UDF
        auto childSignature = child->getHashBasedSignature();

        auto pythonUDFDescriptor = getUDFDescriptor()->as<Catalogs::UDF::PythonUDFDescriptor>(getUDFDescriptor());
        auto& functionName = pythonUDFDescriptor->getMethodName();
        auto& functionString = pythonUDFDescriptor->getFunctionString();
        signatureStream << "PYTHON_UDF(functionName=" + functionName + ", functionString=" + functionString + ")."
                        << *childSignature.begin()->second.begin();

        //Update the signature
        auto hashCode = hashGenerator(signatureStream.str());
        hashBasedSignature[hashCode] = {signatureStream.str()};
    }
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
    return other->instanceOf<UDFLogicalOperator>()
        && *udfDescriptor == *other->as<UDFLogicalOperator>()->udfDescriptor;
}

bool UDFLogicalOperator::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<UDFLogicalOperator>()->id;
}
}// namespace NES