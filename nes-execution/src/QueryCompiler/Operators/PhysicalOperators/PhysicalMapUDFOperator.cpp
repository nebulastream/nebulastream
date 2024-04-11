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

#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapUDFOperator.hpp>
#include <sstream>

namespace NES::QueryCompilation::PhysicalOperators {
PhysicalMapUDFOperator::PhysicalMapUDFOperator(OperatorId id,
                                               StatisticId statisticId,
                                               const SchemaPtr& inputSchema,
                                               const SchemaPtr& outputSchema,
                                               const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)),
      udfDescriptor(std::move(udfDescriptor)) {}

PhysicalOperatorPtr PhysicalMapUDFOperator::create(StatisticId statisticId,
                                                   const SchemaPtr& inputSchema,
                                                   const SchemaPtr& outputSchema,
                                                   const Catalogs::UDF::UDFDescriptorPtr udfDescriptor) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, udfDescriptor);
}

PhysicalOperatorPtr PhysicalMapUDFOperator::create(OperatorId id,
                                                   StatisticId statisticId,
                                                   const SchemaPtr& inputSchema,
                                                   const SchemaPtr& outputSchema,
                                                   const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor) {
    return std::make_shared<PhysicalMapUDFOperator>(id, statisticId, inputSchema, outputSchema, udfDescriptor);
}

std::string PhysicalMapUDFOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalMapUDFOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (udfDescriptor != nullptr) {
        out << udfDescriptor->generateInferStringSignature().str();
    }
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalMapUDFOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema, udfDescriptor);
    result->addAllProperties(properties);
    return result;
}

Catalogs::UDF::UDFDescriptorPtr PhysicalMapUDFOperator::getUDFDescriptor() { return udfDescriptor; }
}// namespace NES::QueryCompilation::PhysicalOperators