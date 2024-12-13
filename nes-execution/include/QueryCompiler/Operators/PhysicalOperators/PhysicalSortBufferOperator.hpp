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

#pragma once

#include <Execution/Operators/SortBuffer.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
// #include <Execution/Operators/SortBuffer/SortBufferOperatorHandler.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical sort buffer operator.
 */
class PhysicalSortBufferOperator : public PhysicalUnaryOperator, public AbstractScanOperator, public AbstractEmitOperator
{
public:
    /**
     * @brief Constructor for the physical sort buffer operator
     * @param id operator id
     * @param inputSchema input schema for the sort buffer operator
     */
    PhysicalSortBufferOperator(
        OperatorId id,
        const SchemaPtr& inputSchema,
        // const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& sortBufferOpHandler,
        const Nautilus::Record::RecordFieldIdentifier& sortFieldIdentifier,
        const Runtime::Execution::Operators::SortOrder sortOrder);

    /**
     * @brief Creates a physical sort buffer operator
     * @param id operator id
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(
        OperatorId id,
        const SchemaPtr& inputSchema,
        //   const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& sortBufferOpHandler,
        const Nautilus::Record::RecordFieldIdentifier& sortFieldIdentifier,
        const Runtime::Execution::Operators::SortOrder sortOrder);

    /**
     * @brief Creates a physical sort buffer operator
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(
        SchemaPtr inputSchema,
        //   const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& sortBufferOpHandler,
        const Nautilus::Record::RecordFieldIdentifier& sortFieldIdentifier,
        const Runtime::Execution::Operators::SortOrder sortOrder);

    // const Runtime::Execution::Operators::SortBufferOperatorHandlerPtr& getSortBufferOperatorHandler() const;

    Nautilus::Record::RecordFieldIdentifier getSortFieldIdentifier() const;

    Runtime::Execution::Operators::SortOrder getSortOrder() const;

    std::string toString() const override;

    OperatorPtr copy() override;

protected:
    // Runtime::Execution::Operators::SortBufferOperatorHandlerPtr sortBufferOpHandler;
    Nautilus::Record::RecordFieldIdentifier sortFieldIdentifier;
    Runtime::Execution::Operators::SortOrder sortOrder;
};
} // namespace NES::QueryCompilation::PhysicalOperators
