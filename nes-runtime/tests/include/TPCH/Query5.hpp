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
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterEqualsExpression.hpp>
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
                                        Runtime::BufferManagerPtr) {
        PipelinePlan plan;
//        auto joinHandler = createPipeline1(plan, tables);
//        auto joinHandler2 = createPipeline2(plan, tables, joinHandler);
//        createPipeline3(plan, tables, joinHandler2);
        createP2(plan, tables);
        return plan;
    }

    static Runtime::Execution::OperatorHandlerPtr
    createP1(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto& customers = tables[TPCHTable::Customer];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        auto customersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(customers->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"c_custkey",
                                                                                 "c_nationkey"};
        auto orderScan = std::make_shared<Operators::Scan>(std::move(customersMemoryProviderPtr), ordersProjection);

        // Join Probe with orders
        std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("c_custkey")};
        std::vector<Record::RecordFieldIdentifier> joinProbeResults = {"o_orderkey",
                                                                       "c_nationkey"};

        auto customersJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                   ordersProbeKeys,
                                                                   std::vector<PhysicalTypePtr>{integerType},
                                                                   std::vector<Record::RecordFieldIdentifier>(),
                                                                   std::vector<PhysicalTypePtr>(),
                                                                   std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        orderScan->setChild(customersJoinProbe);
        // create order_customersJoinBuild pipeline
        auto orderCustomersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        orderCustomersJoinBuild->setRootOperator(orderScan);
        auto joinHandler2 = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {joinHandler2};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderCustomersJoinBuild, pipeline2Context);
        return joinHandler2;
    }

    static Runtime::Execution::OperatorHandlerPtr
    createP2(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto& orders = tables[TPCHTable::Orders];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();

        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        auto ordersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(orders->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"o_orderdate",
                                                                                 "o_shippriority",
                                                                                 "o_custkey",
                                                                                 "o_orderkey"};
        auto orderScan = std::make_shared<Operators::Scan>(std::move(ordersMemoryProviderPtr), ordersProjection);

        /*
        *   o_orderdate >= date '1994-01-01'
        *   and o_orderdate < date '1995-01-01'
        */
        auto const_1994_01_01 = std::make_shared<ConstantInt32ValueExpression>(19940101);
        auto const_1995_01_01 = std::make_shared<ConstantInt32ValueExpression>(19950101);
        auto readOrderDate = std::make_shared<ReadFieldExpression>("o_orderdate");

        auto lessThanExpression1 = std::make_shared<GreaterEqualsExpression>(const_1994_01_01, readOrderDate);
        auto lessThanExpression2 = std::make_shared<LessThanExpression>(readOrderDate, const_1995_01_01);
        auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);

        auto orderDateSelection = std::make_shared<Selection>(andExpression);
        orderScan->setChild(orderDateSelection);

        // build ht for first join
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

    static Runtime::Execution::OperatorHandlerPtr
    createP3(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto& lineItems = tables[TPCHTable::LineItem];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        auto lineItemsMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineItems->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"l_suppkey",
                                                                                 "l_orderkey"};
        auto lineItemScan = std::make_shared<Operators::Scan>(std::move(lineItemsMemoryProviderPtr), ordersProjection);

        // build hash table for lineItem
        auto readL_orderkey = std::make_shared<ReadFieldExpression>("l_orderkey");
        auto joinBuildOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                  std::vector<Expressions::ExpressionPtr>{readL_orderkey},
                                                                  std::vector<PhysicalTypePtr>{integerType},
                                                                  std::vector<Expressions::ExpressionPtr>(),
                                                                  std::vector<PhysicalTypePtr>(),
                                                                  std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        lineItemScan->setChild(joinBuildOp);

        // create orderJoinBuildPipeline pipeline
        auto lineItemJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
        lineItemJoinBuildPipeline->setRootOperator(lineItemScan);
        std::vector<Runtime::Execution::OperatorHandlerPtr> joinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(joinHandler);
        plan.appendPipeline(lineItemJoinBuildPipeline, pipeline1Context);
        return joinHandler[0];
    }

    static Runtime::Execution::OperatorHandlerPtr
    createP4(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto& suppliers = tables[TPCHTable::Supplier];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        auto suppliersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(suppliers->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"s_nationkey",
                                                                                 "s_suppkey"};
        auto suppliersScan = std::make_shared<Operators::Scan>(std::move(suppliersMemoryProviderPtr), ordersProjection);

        // build hash table for suppliers
        auto readS_suppkey = std::make_shared<ReadFieldExpression>("s_suppkey");
        auto joinBuildOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                       std::vector<Expressions::ExpressionPtr>{readS_suppkey},
                                                                       std::vector<PhysicalTypePtr>{integerType},
                                                                       std::vector<Expressions::ExpressionPtr>(),
                                                                       std::vector<PhysicalTypePtr>(),
                                                                       std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        suppliersScan->setChild(joinBuildOp);

        // create orderJoinBuildPipeline pipeline
        auto lineItemJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
        lineItemJoinBuildPipeline->setRootOperator(suppliersScan);
        std::vector<Runtime::Execution::OperatorHandlerPtr> joinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(joinHandler);
        plan.appendPipeline(lineItemJoinBuildPipeline, pipeline1Context);
        return joinHandler[0];
    }

    static Runtime::Execution::OperatorHandlerPtr
        createP5(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto& nations = tables[TPCHTable::Nation];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        auto nationsMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(nations->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"n_nationkey",
                                                                                 "n_regionkey"};
        auto nationsScan = std::make_shared<Operators::Scan>(std::move(nationsMemoryProviderPtr), ordersProjection);

        // build hash table for suppliers
        auto readN_nationkey = std::make_shared<ReadFieldExpression>("n_nationkey");
        auto joinBuildOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                       std::vector<Expressions::ExpressionPtr>{readN_nationkey},
                                                                       std::vector<PhysicalTypePtr>{integerType},
                                                                       std::vector<Expressions::ExpressionPtr>(),
                                                                       std::vector<PhysicalTypePtr>(),
                                                                       std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        nationsScan->setChild(joinBuildOp);

        // create orderJoinBuildPipeline pipeline
        auto lineItemJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
        lineItemJoinBuildPipeline->setRootOperator(nationsScan);
        std::vector<Runtime::Execution::OperatorHandlerPtr> joinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(joinHandler);
        plan.appendPipeline(lineItemJoinBuildPipeline, pipeline1Context);
        return joinHandler[0];
    }

    static Runtime::Execution::OperatorHandlerPtr
    createP6(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto& regions = tables[TPCHTable::Region];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        auto regionsMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(regions->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"r_regionkey",
                                                                                 "r_name"};
        auto nationsScan = std::make_shared<Operators::Scan>(std::move(regionsMemoryProviderPtr), ordersProjection);

        // Selection r_name = 'ASIA' (currently modeled as 1)
        auto asia = std::make_shared<ConstantInt32ValueExpression>(1);
        auto readR_name = std::make_shared<ReadFieldExpression>("r_name");
        auto equalsExpression = std::make_shared<EqualsExpression>(readR_name, asia);
        auto selection = std::make_shared<Selection>(equalsExpression);
        nationsScan->setChild(selection);

        // build hash table for suppliers
        auto readR_regionkey = std::make_shared<ReadFieldExpression>("r_regionkey");
        auto joinBuildOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                       std::vector<Expressions::ExpressionPtr>{readR_regionkey},
                                                                       std::vector<PhysicalTypePtr>{integerType},
                                                                       std::vector<Expressions::ExpressionPtr>(),
                                                                       std::vector<PhysicalTypePtr>(),
                                                                       std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        nationsScan->setChild(joinBuildOp);

        // create orderJoinBuildPipeline pipeline
        auto lineItemJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
        lineItemJoinBuildPipeline->setRootOperator(nationsScan);
        std::vector<Runtime::Execution::OperatorHandlerPtr> joinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(joinHandler);
        plan.appendPipeline(lineItemJoinBuildPipeline, pipeline1Context);
        return joinHandler[0];
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
