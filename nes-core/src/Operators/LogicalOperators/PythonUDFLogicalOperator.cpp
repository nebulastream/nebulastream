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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
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
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Operators/LogicalOperators/PythonUDFLogicalOperator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Util/Logger/Logger.hpp>
#include <numeric>

namespace NES {

PythonUDFLogicalOperator::PythonUDFLogicalOperator(const Catalogs::UDF::PythonUdfDescriptorPtr& pythonUDFDescriptor,
                                                   OperatorId id)
    : OperatorNode(id), UDFLogicalOperator(pythonUDFDescriptor,id), pythonUDFDescriptor(pythonUDFDescriptor) {}

void PythonUDFLogicalOperator::inferStringSignature() {
    NES_TRACE("PythonUDFLogicalOperator: Inferring String signature for {}", toString());
    NES_ASSERT(children.size() == 1, "PythonUDFLogicalOperator: PythonUDFLogicalOperator should have exactly 1 child.");
    //Infer query signatures for child operator
    auto child = children[0]->as<LogicalOperatorNode>();
    child->inferStringSignature();
    // Infer signature for this operator.
    std::stringstream signatureStream;
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

Catalogs::UDF::PythonUDFDescriptorPtr PythonUDFLogicalOperator::getPythonUDFDescriptor() const { return pythonUDFDescriptor; }

bool PythonUDFLogicalOperator::equal(const NodePtr& other) const {
    return other->instanceOf<PythonUDFLogicalOperator>()
        && *pythonUDFDescriptor == *other->as<PythonUDFLogicalOperator>()->pythonUDFDescriptor;
}

bool PythonUDFLogicalOperator::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<PythonUDFLogicalOperator>()->id;
}
}// namespace NES
#endif// NAUTILUS_PYTHON_UDF_ENABLED