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


#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapJavaUdfOperator.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>


namespace NES::QueryCompilation::PhysicalOperators {
PhysicalMapJavaUdfOperator::PhysicalMapJavaUdfOperator(OperatorId id,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor)
            : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
            javaUdfDescriptor(std::move(javaUdfDescriptor)) {}

PhysicalOperatorPtr
PhysicalMapJavaUdfOperator::create(SchemaPtr inputSchema,
                                   SchemaPtr outputSchema,
                                   Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor) {
    return create(Util::getNextOperatorId(), inputSchema, outputSchema, javaUdfDescriptor);
}

PhysicalOperatorPtr
PhysicalMapJavaUdfOperator::create(OperatorId id,
                                   const SchemaPtr& inputSchema,
                                   const SchemaPtr& outputSchema,
                                   const Catalogs::UDF::JavaUdfDescriptorPtr& javaUdfDescriptor) {
    return std::make_shared<PhysicalMapJavaUdfOperator>(id, inputSchema, outputSchema, javaUdfDescriptor);
}

std::string PhysicalMapJavaUdfOperator::toString() const { return "PhysicalMapJavaUdfOperator"; }

OperatorNodePtr PhysicalMapJavaUdfOperator::copy() { return create(id, inputSchema, outputSchema, javaUdfDescriptor); }

Catalogs::UDF::JavaUdfDescriptorPtr PhysicalMapJavaUdfOperator::getjavaUdfDescriptor() {
    return javaUdfDescriptor;
}
}// namespace NES::QueryCompilation::PhysicalOperators
