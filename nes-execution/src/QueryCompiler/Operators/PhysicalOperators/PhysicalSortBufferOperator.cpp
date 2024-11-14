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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSortBufferOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalSortBufferOperator::PhysicalSortBufferOperator(OperatorId id, 
                                                        // StatisticId statisticId,
                                                        const SchemaPtr& inputSchema,
                                                        // const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& sortBufferOpHandler,
                                                        const Nautilus::Record::RecordFieldIdentifier& sortFieldIdentifier,
                                                        const Runtime::Execution::Operators::SortOrder sortOrder)
    : Operator(id /*,statisticId*/), PhysicalUnaryOperator(id, /*statisticId,*/ inputSchema, inputSchema),
      /*sortBufferOpHandler(sortBufferOpHandler),*/ sortFieldIdentifier(sortFieldIdentifier), sortOrder(sortOrder) {}

PhysicalOperatorPtr PhysicalSortBufferOperator::create(//StatisticId statisticId,
                                                        SchemaPtr inputSchema,
                                                        // const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& sortBufferOpHandler,
                                                        const Nautilus::Record::RecordFieldIdentifier& sortFieldIdentifier,
                                                        const Runtime::Execution::Operators::SortOrder sortOrder) {
    return create(getNextOperatorId(), /*statisticId,*/ std::move(inputSchema), /*sortBufferOpHandler,*/ sortFieldIdentifier, sortOrder);
}
PhysicalOperatorPtr PhysicalSortBufferOperator::create(OperatorId id,
                                                        // StatisticId statisticId,
                                                        const SchemaPtr& inputSchema,
                                                        // const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& sortBufferOpHandler,
                                                        const Nautilus::Record::RecordFieldIdentifier& sortFieldIdentifier,
                                                        const Runtime::Execution::Operators::SortOrder sortOrder) {
    return std::make_shared<PhysicalSortBufferOperator>(id, /*statisticId,*/ inputSchema, /*sortBufferOpHandler,*/ sortFieldIdentifier, sortOrder);
}

// const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& PhysicalSortBufferOperator::getSortBufferOperatorHandler() const {
//     return sortBufferOpHandler;
// }

Nautilus::Record::RecordFieldIdentifier PhysicalSortBufferOperator::getSortFieldIdentifier() const {
    return sortFieldIdentifier;
}

Runtime::Execution::Operators::SortOrder PhysicalSortBufferOperator::getSortOrder() const {
    return sortOrder;
}

std::string PhysicalSortBufferOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalSortBufferOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

OperatorPtr PhysicalSortBufferOperator::copy() {
    auto result = create(id, /*statisticId,*/ inputSchema, /*sortBufferOpHandler,*/ sortFieldIdentifier, sortOrder);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators