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

#include <Operators/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/UDFs/PythonUDFDescriptor.hpp>
#include <sstream>

namespace NES::Catalogs::UDF {

PythonUDFDescriptor::PythonUDFDescriptor(const std::string& functionName,
                                         const std::string& functionString,
                                         const SchemaPtr& inputSchema,
                                         const SchemaPtr& outputSchema)
    : UDFDescriptor(functionName, inputSchema, outputSchema), functionString(functionString) {
    if (functionString.empty()) {
        throw UDFException("Function String of Python UDF must not be empty");
    }
}

bool PythonUDFDescriptor::operator==(const PythonUDFDescriptor& other) const {
    return functionString == other.functionString && getMethodName() == other.getMethodName()
        && getInputSchema()->equals(other.getInputSchema(), true) && getOutputSchema()->equals(other.getOutputSchema(), true);
}

std::stringstream PythonUDFDescriptor::generateInferStringSignature() {
    auto signatureStream = std::stringstream{};
    auto& functionName = getMethodName();
    signatureStream << "PYTHON_UDF(functionName=" + functionName + ", functionString=" + functionString + ")";
    return signatureStream;
}
}// namespace NES::Catalogs::UDF
