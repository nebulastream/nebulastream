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

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalReorderTupleBuffersOperator.hpp>
#include <sstream>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalReorderTupleBuffersOperator::PhysicalReorderTupleBuffersOperator(
    OperatorId id,
    StatisticId statisticId,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)) {}

PhysicalOperatorPtr
PhysicalReorderTupleBuffersOperator::create(OperatorId id,
                                            StatisticId statisticId,
                                            SchemaPtr inputSchema,
                                            SchemaPtr outputSchema) {
    return std::make_shared<PhysicalReorderTupleBuffersOperator>(id,
                  statisticId,
                  std::move(inputSchema),
                  std::move(outputSchema));
}

PhysicalOperatorPtr
PhysicalReorderTupleBuffersOperator::create(StatisticId statisticId,
                                            SchemaPtr inputSchema,
                                            SchemaPtr outputSchema) {
    return create(getNextOperatorId(),
                  statisticId,
                  std::move(inputSchema),
                  std::move(outputSchema));
}

std::string PhysicalReorderTupleBuffersOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalReorderTupleBuffersOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalReorderTupleBuffersOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators
