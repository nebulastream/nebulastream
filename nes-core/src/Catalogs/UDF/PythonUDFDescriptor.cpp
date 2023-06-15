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
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Exceptions/UDFException.hpp>

namespace NES::Catalogs::UDF {

PythonUDFDescriptor::PythonUDFDescriptor(const std::string& functionName, std::string& functionString, const SchemaPtr inputSchema,  const SchemaPtr outputSchema)
    : UDFDescriptor(functionName), functionString(functionString), inputSchema(inputSchema), outputSchema(outputSchema) {

    if (functionName.empty()) {
        throw UDFException("The function name of a Python UDF must not be empty");
    }
    if (functionString.empty()){
        throw UDFException("Function String of Python UDF must not be empty");
    }
    if (inputSchema->empty()) {
        throw UDFException("The input schema of a Python UDF must not be empty");
    }
    if (inputSchema->empty()) {
        throw UDFException("The output schema of a Python UDF must not be empty");
    }
}
bool PythonUDFDescriptor::operator==(const PythonUDFDescriptor& other) const {
    return functionString == other.functionString && getMethodName() == other.getMethodName()
        && inputSchema->equals(other.inputSchema, true) && outputSchema->equals(other.outputSchema, true);
}

void PythonUDFDescriptor::setInputSchema(const SchemaPtr& inputSchema) { PythonUDFDescriptor::inputSchema = inputSchema; }


}// namespace NES::Catalogs::UDF
#endif// NAUTILUS_PYTHON_UDF_ENABLED