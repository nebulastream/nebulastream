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
#ifndef NES_EXECUTION_TESTS_INCLUDE_TPCH_QUERY3_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TPCH_QUERY3_HPP_

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
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinBuild.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TPCH/PipelinePlan.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <Util/TestTupleBuffer.hpp>
namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Query3 {
  public:
    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr) {
        PipelinePlan plan;
        auto joinHandler = createPipeline1(plan, tables);
        auto joinHandler2 = createPipeline2(plan, tables, joinHandler);
        createPipeline3(plan, tables, joinHandler2);

        return plan;
    }

    static Runtime::Execution::OperatorHandlerPtr
    createPipeline1(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
        auto& customers = tables[TPCHTable::Customer];

        auto c_scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(customers->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> customersProjection = {"c_mksegment", "c_custkey"};
        auto customersScan = std::make_shared<Operators::Scan>(std::move(c_scanMemoryProviderPtr), customersProjection);

        // c_mksegment = 'BUILDING' -> currently modeled as 1
        auto BUILDING = std::make_shared<ConstantInt32ValueExpression>(1);
        auto readC_mktsegment = std::make_shared<ReadFieldExpression>("c_mksegment");
        auto equalsExpression = std::make_shared<EqualsExpression>(readC_mktsegment, BUILDING);
        auto selection = std::make_shared<Selection>(equalsExpression);
        customersScan->setChild(selection);

        // build ht for first join
        auto readC_key = std::make_shared<ReadFieldExpression>("c_custkey");
        auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                  std::vector<Expressions::ExpressionPtr>{readC_key},
                                                                  std::vector<PhysicalTypePtr>{integerType},
                                                                  std::vector<Expressions::ExpressionPtr>(),
                                                                  std::vector<PhysicalTypePtr>(),
                                                                  std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        selection->setChild(joinOp);

        // create customerJoinBuildPipeline pipeline
        auto customerJoinBuildPipeline = std::make_shared<PhysicalOperatorPipeline>();
        customerJoinBuildPipeline->setRootOperator(customersScan);
        std::vector<Runtime::Execution::OperatorHandlerPtr> joinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(joinHandler);
        plan.appendPipeline(customerJoinBuildPipeline, pipeline1Context);
        return joinHandler[0];
    }

    static std::shared_ptr<BatchJoinHandler>
    createPipeline2(PipelinePlan& plan,
                    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                    const Runtime::Execution::OperatorHandlerPtr& joinHandler) {
        auto& orders = tables[TPCHTable::Orders];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();

        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /**
        * Pipeline 2 with scan orders -> selection -> JoinPrope with customers from pipeline 1
        */
        auto ordersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(orders->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"o_orderdate",
                                                                                 "o_shippriority",
                                                                                 "o_custkey",
                                                                                 "o_orderkey"};
        auto orderScan = std::make_shared<Operators::Scan>(std::move(ordersMemoryProviderPtr), ordersProjection);

        //  o_orderdate < date '1995-03-15'
        auto const_1995_03_15 = std::make_shared<ConstantInt32ValueExpression>(19950315);
        auto readO_orderdate = std::make_shared<ReadFieldExpression>("o_orderdate");
        auto orderDateSelection =
            std::make_shared<Selection>(std::make_shared<LessThanExpression>(readO_orderdate, const_1995_03_15));
        orderScan->setChild(orderDateSelection);

        // join probe with customers
        std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
        std::vector<IR::Types::StampPtr> valueStamps = {};
        std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};

        std::vector<Record::RecordFieldIdentifier> joinProbeResults = {"o_custkey",
                                                                       "o_orderkey",
                                                                       "o_orderdate",
                                                                       "o_shippriority"};
        auto customersJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                   ordersProbeKeys,
                                                                   std::vector<PhysicalTypePtr>{integerType},
                                                                   std::vector<Record::RecordFieldIdentifier>(),
                                                                   std::vector<PhysicalTypePtr>(),
                                                                   std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        orderDateSelection->setChild(customersJoinProbe);

        // join build for order_customers
        std::vector<ExpressionPtr> order_customersJoinBuildKeys = {std::make_shared<ReadFieldExpression>("o_orderkey")};
        std::vector<ExpressionPtr> order_customersJoinBuildValues = {std::make_shared<ReadFieldExpression>("o_orderdate"),
                                                                     std::make_shared<ReadFieldExpression>("o_shippriority")};

        auto order_customersJoinBuild =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        order_customersJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        order_customersJoinBuildValues,
                                                        std::vector<PhysicalTypePtr>{integerType, integerType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        customersJoinProbe->setChild(order_customersJoinBuild);

        // create order_customersJoinBuild pipeline
        auto orderCustomersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        orderCustomersJoinBuild->setRootOperator(orderScan);
        auto joinHandler2 = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {joinHandler, joinHandler2};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderCustomersJoinBuild, pipeline2Context);
        return joinHandler2;
    }

    static void createPipeline3(PipelinePlan& plan,
                                std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                const std::shared_ptr<BatchJoinHandler>& joinHandler) {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();

        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /**
   * Pipeline 3 with scan lineitem -> selection -> JoinProbe with order_customers from pipeline 2 -> aggregation
   */
        auto lineitemsMP = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> lineItemProjection = {"l_orderkey",
                                                                                   "l_extendedprice",
                                                                                   "l_discount",
                                                                                   "l_shipdate"};
        auto lineitemsScan = std::make_shared<Operators::Scan>(std::move(lineitemsMP), lineItemProjection);

        //   date '1995-03-15' < l_shipdate
        auto readL_shipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
        auto const_1995_03_15 = std::make_shared<ConstantInt32ValueExpression>(19950315);
        auto shipDateSelection =
            std::make_shared<Selection>(std::make_shared<LessThanExpression>(const_1995_03_15, readL_shipdate));
        lineitemsScan->setChild(shipDateSelection);

        // join probe with customers

        //  l_orderkey,
        auto l_orderkey = std::make_shared<ReadFieldExpression>("l_orderkey");
        std::vector<ExpressionPtr> lineitemProbeKeys = {l_orderkey};

        std::vector<Record::RecordFieldIdentifier> orderProbeFieldNames = {"o_shippriority", "o_orderdate"};

        auto lineitemJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                  lineitemProbeKeys,
                                                                  std::vector<PhysicalTypePtr>{integerType},
                                                                  orderProbeFieldNames,
                                                                  std::vector<PhysicalTypePtr>{integerType, integerType},
                                                                  std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        shipDateSelection->setChild(lineitemJoinProbe);

        //  sum(l_extendedprice * (1 - l_discount)) as revenue,
        auto l_extendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
        auto l_discountField = std::make_shared<ReadFieldExpression>("l_discount");
        auto oneConst = std::make_shared<ConstantFloatValueExpression>(1.0f);
        auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
        auto revenueExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
        auto sumRevenue =
            std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType, revenueExpression, "sum_revenue");
        auto readO_orderdate = std::make_shared<ReadFieldExpression>("o_orderdate");
        std::vector<Expressions::ExpressionPtr> keyFields = {l_orderkey,
                                                             readO_orderdate,
                                                             std::make_shared<ReadFieldExpression>("o_shippriority")};
        std::vector<Expressions::ExpressionPtr> aggregationExpressions = {revenueExpression};
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {sumRevenue};

        PhysicalTypePtr smallType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt8());

        auto aggregation = std::make_shared<Operators::BatchKeyedAggregation>(
            1 /*handler index*/,
            keyFields,
            std::vector<PhysicalTypePtr>{integerType, integerType, integerType},
            aggregationFunctions,
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        lineitemJoinProbe->setChild(aggregation);

        // create lineitems_ordersJoinBuild pipeline
        auto lineitems_ordersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        lineitems_ordersJoinBuild->setRootOperator(lineitemsScan);
        auto aggHandler = std::make_shared<Operators::BatchKeyedAggregationHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers2 = {joinHandler, aggHandler};
        auto pipeline3Context = std::make_shared<MockedPipelineExecutionContext>(handlers2);
        plan.appendPipeline(lineitems_ordersJoinBuild, pipeline3Context);
    }
};

}// namespace NES::Runtime::Execution
#endif // NES_EXECUTION_TESTS_INCLUDE_TPCH_QUERY3_HPP_
