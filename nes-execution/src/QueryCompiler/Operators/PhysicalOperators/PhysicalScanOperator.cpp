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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalScanOperator::PhysicalScanOperator(OperatorId id, StatisticId statisticId, const SchemaPtr& outputSchema)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, outputSchema, outputSchema) {}

PhysicalOperatorPtr PhysicalScanOperator::create(StatisticId statisticId, SchemaPtr outputSchema) {
    return create(getNextOperatorId(), statisticId, std::move(outputSchema));
}
PhysicalOperatorPtr PhysicalScanOperator::create(OperatorId id, StatisticId statisticId, const SchemaPtr& outputSchema) {
    return std::make_shared<PhysicalScanOperator>(id, statisticId, outputSchema);
}

std::string PhysicalScanOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalScanOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

OperatorPtr PhysicalScanOperator::copy() {
    auto result = create(id, statisticId, outputSchema);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators