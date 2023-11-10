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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_

#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinBuild.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TPCH/PipelinePlan.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Query5 {
  public:
    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr bm) {
        PipelinePlan plan;
        auto custOrderJoinHandler = createOrderPipeline(plan, tables);
        createCustPipeline(plan, tables, custOrderJoinHandler, bm);
        return plan;
    }

    // Order Pipeline: Scan->Selection(date)->JoinBuild
    static Runtime::Execution::OperatorHandlerPtr
    createOrderPipeline(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        /*
         * 1. Scan
         */
        auto& orders = tables[TPCHTable::Orders];
        auto ordersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(orders->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"o_orderdate",
                                                                                 "o_shippriority",
                                                                                 "o_custkey",
                                                                                 "o_orderkey"};
        auto orderScan = std::make_shared<Operators::Scan>(std::move(ordersMemoryProviderPtr), ordersProjection);

        /*
         * 2. Selection: o_orderdate >= date '1994-01-01' and o_orderdate < date '1995-01-01'
         */
        auto const_1994_01_01 = std::make_shared<ConstantInt32ValueExpression>(19940101);
        auto const_1995_01_01 = std::make_shared<ConstantInt32ValueExpression>(19950101);
        auto readOrderDate = std::make_shared<ReadFieldExpression>("o_orderdate");

        auto lessThanExpression1 = std::make_shared<GreaterEqualsExpression>(const_1994_01_01, readOrderDate);
        auto lessThanExpression2 = std::make_shared<LessThanExpression>(readOrderDate, const_1995_01_01);
        auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);

        auto orderDateSelection = std::make_shared<Selection>(andExpression);
        orderScan->setChild(orderDateSelection);

        /*
         * 3. JoinBuild
         */
        auto readO_custkey = std::make_shared<ReadFieldExpression>("o_custkey");
        auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                  std::vector<Expressions::ExpressionPtr>{readO_custkey},
                                                                  std::vector<PhysicalTypePtr>{integerType},
                                                                  std::vector<Expressions::ExpressionPtr>(),
                                                                  std::vector<PhysicalTypePtr>(),
                                                                  std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        orderDateSelection->setChild(joinOp);

        // create orderJoinBuildPipeline pipeline
        auto orderJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
        orderJoinBuildPipeline->setRootOperator(orderScan);
        std::vector<Runtime::Execution::OperatorHandlerPtr> joinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(joinHandler);
        plan.appendPipeline(orderJoinBuildPipeline, pipeline1Context);
        return joinHandler[0];
    }

    // Customer Pipeline: Scan->JoinProbe(with order)->JoinBuild
    static void createCustPipeline(PipelinePlan& plan,
                            std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                            Runtime::Execution::OperatorHandlerPtr custOrderJoinHandler,
                            Runtime::BufferManagerPtr bm) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        /*
         * 1. Scan
         */
        auto& customers = tables[TPCHTable::Customer];
        auto customersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(customers->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> customersProjection = {"c_orderkey", "c_custkey", "c_nationkey"};
        auto customerScan = std::make_shared<Operators::Scan>(std::move(customersMemoryProviderPtr), customersProjection);

        /*
         * 2. Join Probe with orders
         */
        std::vector<ExpressionPtr> custProbeKeys = {std::make_shared<ReadFieldExpression>("c_custkey")};
        auto customersJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                   custProbeKeys,
                                                                   std::vector<PhysicalTypePtr>{integerType},
                                                                   std::vector<Record::RecordFieldIdentifier>(),
                                                                   std::vector<PhysicalTypePtr>(),
                                                                   std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        customerScan->setChild(customersJoinProbe);

        // emit operator
        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        resultSchema->addField("revenue", BasicType::FLOAT32);
        auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        customersJoinProbe->setChild(emit);

        // create emit pipeline
        auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
        emitPipeline->setRootOperator(customersJoinProbe);
        std::vector<OperatorHandlerPtr> aggregationHandler = {custOrderJoinHandler};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(emitPipeline, pipeline2Context);
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
