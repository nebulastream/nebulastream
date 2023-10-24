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
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>

namespace NES::Catalogs::UDF {

UDFDescriptor::UDFDescriptor(const std::string& methodName, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema)
    : methodName(methodName), inputSchema(inputSchema), outputSchema(outputSchema) {
    if (methodName.empty()) {
        throw UDFException("The method name of a UDF must not be empty");
    }
    if (outputSchema->empty()) {
        throw UDFException("The output schema of a UDF must not be empty");
    }

    // We allow the input schema to be empty for now so that we don't have to serialize it in the client
}

void UDFDescriptor::setInputSchema(const SchemaPtr& inputSchema) { UDFDescriptor::inputSchema = inputSchema; }

bool UDFDescriptor::operator==(const UDFDescriptor& other) const {
    return getMethodName() == other.getMethodName() && inputSchema->equals(other.inputSchema, true)
        && outputSchema->equals(other.outputSchema, true);
}
}// namespace NES::Catalogs::UDF