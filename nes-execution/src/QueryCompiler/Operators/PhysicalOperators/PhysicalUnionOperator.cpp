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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <sstream>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalUnionOperator::create(OperatorId id, StatisticId statisticId, const SchemaPtr& schema) {
    return create(id, statisticId, schema, schema, schema);
}

PhysicalOperatorPtr PhysicalUnionOperator::create(OperatorId id,
                                                  StatisticId statisticId,
                                                  const SchemaPtr& leftSchema,
                                                  const SchemaPtr& rightSchema,
                                                  const SchemaPtr& outputSchema) {
    return std::make_shared<PhysicalUnionOperator>(id, statisticId, leftSchema, rightSchema, outputSchema);
}

PhysicalOperatorPtr PhysicalUnionOperator::create(StatisticId statisticId, const SchemaPtr& schema) { return create(getNextOperatorId(), statisticId, schema); }

PhysicalUnionOperator::PhysicalUnionOperator(OperatorId id,
                                             StatisticId statisticId,
                                             const SchemaPtr& leftSchema,
                                             const SchemaPtr& rightSchema,
                                             const SchemaPtr& outputSchema)
    : Operator(id), PhysicalBinaryOperator(id, statisticId, leftSchema, rightSchema, outputSchema) {}

std::string PhysicalUnionOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalUnionOperator:\n";
    return out.str();
}
OperatorPtr PhysicalUnionOperator::copy() { return create(id, statisticId, leftInputSchema, rightInputSchema, outputSchema); }

}// namespace NES::QueryCompilation::PhysicalOperators