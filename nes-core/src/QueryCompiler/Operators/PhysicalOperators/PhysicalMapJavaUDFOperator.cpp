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

#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapJavaUDFOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
PhysicalMapJavaUDFOperator::PhysicalMapJavaUDFOperator(OperatorId id,
                                                       const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const Catalogs::UDF::JavaUDFDescriptorPtr& javaUDFDescriptor)
    : OperatorNode(id), PhysicalUDFOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(javaUDFDescriptor)), javaUDFDescriptor(std::move(javaUDFDescriptor)) {}

PhysicalOperatorPtr PhysicalMapJavaUDFOperator::create(const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor) {
    return create(Util::getNextOperatorId(), inputSchema, outputSchema, javaUDFDescriptor);
}

PhysicalOperatorPtr PhysicalMapJavaUDFOperator::create(OperatorId id,
                                                       const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const Catalogs::UDF::JavaUDFDescriptorPtr& javaUDFDescriptor) {
    NES_DEBUG("Generating Physcial Operator");
    return std::make_shared<PhysicalMapJavaUDFOperator>(id, inputSchema, outputSchema, javaUDFDescriptor);
}

OperatorNodePtr PhysicalMapJavaUDFOperator::copy() { return create(id, inputSchema, outputSchema, javaUDFDescriptor);}

std::string PhysicalMapJavaUDFOperator::toString() const { return "PhysicalMapJavaUDFOperator"; }

}// namespace NES::QueryCompilation::PhysicalOperators
