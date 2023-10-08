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

#include <Util/PythonUDFDescriptorBuilder.hpp>

namespace NES::Catalogs::UDF {
PythonUDFDescriptorPtr PythonUDFDescriptorBuilder::build() {
    return PythonUDFDescriptor::create(functionName, functionString, modulesToImport, pythonCompiler, inputSchema, outputSchema);
}

PythonUDFDescriptorBuilder& PythonUDFDescriptorBuilder::setFunctionName(const std::string& newFunctionName) {
    this->functionName = newFunctionName;
    return *this;
}

PythonUDFDescriptorBuilder& PythonUDFDescriptorBuilder::setFunctionString(const std::string& newFunctionString) {
    this->functionString = newFunctionString;
    return *this;
}

PythonUDFDescriptorBuilder& PythonUDFDescriptorBuilder::setModulesToImport(const std::map<std::string, std::string> newModulesToImport) {
    this->modulesToImport = newModulesToImport;
    return *this;
}


PythonUDFDescriptorBuilder& PythonUDFDescriptorBuilder::setPythonCompiler(const std::string& newPythonCompiler) {
    this->pythonCompiler = newPythonCompiler;
    return *this;
}

PythonUDFDescriptorBuilder& PythonUDFDescriptorBuilder::setInputSchema(const SchemaPtr& newInputSchema) {
    this->inputSchema = newInputSchema;
    return *this;
}

PythonUDFDescriptorBuilder& PythonUDFDescriptorBuilder::setOutputSchema(const SchemaPtr& newOutputSchema) {
    this->outputSchema = newOutputSchema;
    return *this;
}

PythonUDFDescriptorPtr PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor() {
    std::string functionName = "udf_function";
    std::string functionString = "def udf_function(x):\n\ty = x + 10\n\treturn y\n";
    std::string pythonCompiler = "default"; // default compiler
    SchemaPtr inputSchema = std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64());
    SchemaPtr outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createUInt64());
    return PythonUDFDescriptorBuilder{}
        .setFunctionName(functionName)
        .setFunctionString(functionString)
        .setPythonCompiler(pythonCompiler)
        .setInputSchema(inputSchema)
        .setOutputSchema(outputSchema)
        .build();
}

}// namespace NES::Catalogs::UDF