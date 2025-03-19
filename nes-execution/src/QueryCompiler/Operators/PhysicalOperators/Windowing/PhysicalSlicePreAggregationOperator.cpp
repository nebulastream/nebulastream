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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalSlicePreAggregationOperator::create(OperatorId id,
                                                                StatisticId statisticId,
                                                                const SchemaPtr& inputSchema,
                                                                const SchemaPtr& outputSchema,
                                                                const Windowing::LogicalWindowDescriptorPtr& windowDefinition,
                                       Runtime::Execution::Operators::KeyedSlicePreAggregationHandlerPtr preAggHandler) {
    return std::make_shared<PhysicalSlicePreAggregationOperator>(id, statisticId, inputSchema, outputSchema, windowDefinition, preAggHandler);
}

PhysicalSlicePreAggregationOperator::PhysicalSlicePreAggregationOperator(OperatorId id,
                                                                         StatisticId statisticId,
                                                                         SchemaPtr inputSchema,
                                                                         SchemaPtr outputSchema,
                                                                         Windowing::LogicalWindowDescriptorPtr windowDefinition,
                                       Runtime::Execution::Operators::KeyedSlicePreAggregationHandlerPtr preAggHandler)
    : Operator(id),
      PhysicalWindowOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema), std::move(windowDefinition)), preAggHandler(preAggHandler) {};

std::string PhysicalSlicePreAggregationOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalWindowPreAggregationOperator:\n";
    out << PhysicalWindowOperator::toString();
    return out.str();
}

OperatorPtr PhysicalSlicePreAggregationOperator::copy() {
    return create(id, statisticId, inputSchema, outputSchema, windowDefinition, preAggHandler);
}

Runtime::Execution::Operators::KeyedSlicePreAggregationHandlerPtr PhysicalSlicePreAggregationOperator::getPreAggregationHandler() const {
    return preAggHandler;
}
}// namespace NES::QueryCompilation::PhysicalOperators
