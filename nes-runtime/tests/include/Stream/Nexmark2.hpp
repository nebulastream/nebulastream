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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_NX2_MAP_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_NX2_MAP_HPP_

#include "Execution/Expressions/LogicalExpressions/EqualsExpression.hpp"
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
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
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
class Nexmark2 {
  public:
    static PipelinePlan getPipelinePlan(std::unique_ptr<NES::Runtime::Table>& table, Runtime::BufferManagerPtr bm) {

        auto schema = table->getLayout()->getSchema();
        PipelinePlan plan;
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {};
        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(table->getLayout()),
            projections);

        auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        // .filter(Attribute("auctionId") == 1007 || Attribute("auctionId") == 1020 || Attribute("auctionId") == 2001 || Attribute("auctionId") == 2019 || Attribute("auctionId") == 2087).

        auto auctionId = std::make_shared<Expressions::ReadFieldExpression>("auctionId");
        auto id1 = std::make_shared<Expressions::ConstantInt64ValueExpression>(1007);
        auto e1 = std::make_shared<Expressions::EqualsExpression>(auctionId, id1);
        auto id2 = std::make_shared<Expressions::ConstantInt64ValueExpression>(1020);
        auto e2 = std::make_shared<Expressions::EqualsExpression>(auctionId, id2);
        auto id3 = std::make_shared<Expressions::ConstantInt64ValueExpression>(2001);
        auto e3 = std::make_shared<Expressions::EqualsExpression>(auctionId, id3);
        auto id4 = std::make_shared<Expressions::ConstantInt64ValueExpression>(2019);
        auto e4 = std::make_shared<Expressions::EqualsExpression>(auctionId, id4);
        auto id5 = std::make_shared<Expressions::ConstantInt64ValueExpression>(2087);
        auto e5 = std::make_shared<Expressions::EqualsExpression>(auctionId, id5);
        auto o1 = std::make_shared<Expressions::OrExpression>(e1, e2);
        auto o2 = std::make_shared<Expressions::OrExpression>(o1, e3);
        auto o3 = std::make_shared<Expressions::OrExpression>(o2, e4);
        auto o4 = std::make_shared<Expressions::OrExpression>(o3, e5);

        std::shared_ptr<Operators::ExecutableOperator> filterOperator = std::make_shared<Operators::Selection>(o4);
        scan->setChild(filterOperator);
        auto resultLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(resultLayout));
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        filterOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scan);
        auto pipelineContext = std::make_shared<BlackholePipelineExecutionContext>();
        plan.appendPipeline(pipeline, pipelineContext);
        return plan;
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_NX2_MAP_HPP_
