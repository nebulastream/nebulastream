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

#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Exceptions/UDFException.hpp>
#include <sstream>

namespace NES::Catalogs::UDF {

PythonUDFDescriptor::PythonUDFDescriptor(const std::string& functionName,
                                         const std::string& functionString,
                                         const std::map<std::string, std::string>& modulesToImport,
                                         const std::string& pythonCompiler,
                                         const SchemaPtr& inputSchema,
                                         const SchemaPtr& outputSchema)
    : UDFDescriptor(functionName, inputSchema, outputSchema), functionString(functionString), modulesToImport(modulesToImport), pythonCompiler(pythonCompiler) {
    if (functionString.empty()) {
        throw UDFException("Function String of Python UDF must not be empty");
    }
}
bool PythonUDFDescriptor::operator==(const PythonUDFDescriptor& other) const {
    return functionString == other.functionString && getMethodName() == other.getMethodName()
        && getInputSchema()->equals(other.getInputSchema(), true) && getInputSchema()->equals(other.getInputSchema(), true);
}
std::stringstream PythonUDFDescriptor::generateInferStringSignature() {
    auto signatureStream = std::stringstream{};
    auto& functionName = getMethodName();
    signatureStream << "PYTHON_UDF(functionName=" + functionName + ", functionString=" + functionString + ", pythonCompiler=" + pythonCompiler + ")";
    return signatureStream;
}
}// namespace NES::Catalogs::UDF
