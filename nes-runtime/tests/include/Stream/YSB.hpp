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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_YSB_MAP_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_YSB_MAP_HPP_

#include "Execution/Expressions/LogicalExpressions/EqualsExpression.hpp"
#include "Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp"
#include "Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp"
#include "Nautilus/Interface/Hash/MurMur3HashFunction.hpp"
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
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
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
class YSB {
  public:
    static PipelinePlan getPipelinePlan(std::unique_ptr<NES::Runtime::Table>& table, Runtime::BufferManagerPtr) {
        DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
        auto schema = table->getLayout()->getSchema();
        PipelinePlan plan;
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {"event_type", "campaign_id"};
        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(table->getLayout()),
            projections);

        auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        // Query::from("ysb").filter(Attribute("event_type") < 1)

        auto event = std::make_shared<Expressions::ReadFieldExpression>("event_type");
        auto eventType = std::make_shared<Expressions::ConstantInt64ValueExpression>(1);
        auto e1 = std::make_shared<Expressions::LessThanExpression>(event, eventType);

        std::shared_ptr<Operators::ExecutableOperator> filterOperator = std::make_shared<Operators::Selection>(e1);
        scan->setChild(filterOperator);

        // .window(TumblingWindow::of(EventTime(RecordCreationTs()), Seconds(30))).byKey(Attribute("campaign_id")).apply(Sum(Attribute("user_id")))

        auto readKey = std::make_shared<Expressions::ReadFieldExpression>("campaign_id");
        auto aggregationResultFieldName = "test$sum";
        PhysicalTypePtr integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
        std::vector<Expressions::ExpressionPtr> keyFields = {readKey};
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
            std::make_shared<Aggregation::CountAggregationFunction>(integerType,
                                                                    integerType,
                                                                    readKey,
                                                                    aggregationResultFieldName)};
        std::vector<PhysicalTypePtr> types = {integerType};
        auto slicePreAggregation =
            std::make_shared<Operators::KeyedSlicePreAggregation>(0 /*handler index*/,
                                                                  std::make_unique<Operators::IngestionTimeFunction>(),
                                                                  keyFields,
                                                                  types,
                                                                  aggregationFunctions,
                                                                  std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        filterOperator->setChild(slicePreAggregation);

        auto sliceStaging = std::make_shared<Operators::KeyedSliceStaging>();
        std::vector<OriginId> origins = {0};
        auto preAggregationHandler = std::make_shared<Operators::KeyedSlicePreAggregationHandler>(10, 10, origins, sliceStaging);
        std::vector<OperatorHandlerPtr> handlers = {preAggregationHandler};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(handlers);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scan);
        // auto pipelineContext = std::make_shared<BlackholePipelineExecutionContext>(handlers);
        plan.appendPipeline(pipeline, pipeline1Context);
        return plan;
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_STREAM_YSB_MAP_HPP_
