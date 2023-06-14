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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_NX1_MAP_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_NX1_MAP_HPP_

#include "Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantDecimalExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <PipelinePlan.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Table.hpp>
#include <TestUtils/BlackholePipelineExecutionContext.hpp>

namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class Nexmark1 {
  public:
    static PipelinePlan getPipelinePlan(std::unique_ptr<NES::Runtime::Table>& table, Runtime::BufferManagerPtr bm) {

        auto schema = table->getLayout()->getSchema();
        PipelinePlan plan;
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {};
        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(table->getLayout()),
            projections);

        auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        // Attribute("price") = Attribute("price") * 89 / 100
        auto price = std::make_shared<Expressions::ReadFieldExpression>("price");
        auto constDollarRate = std::make_shared<Expressions::ConstantDecimalExpression>(2, 89);
        auto euroPrice = std::make_shared<Expressions::MulExpression>(price, constDollarRate);
        auto writeF3 = std::make_shared<Expressions::WriteFieldExpression>("price", euroPrice);

        std::shared_ptr<Operators::ExecutableOperator> mapOperator = std::make_shared<Operators::Map>(writeF3);
        scan->setChild(mapOperator);
        auto resultLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(resultLayout));
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        mapOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scan);
        auto pipelineContext = std::make_shared<BlackholePipelineExecutionContext>();
        plan.appendPipeline(pipeline, pipelineContext);
        return plan;
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_NX1_MAP_HPP_
