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
                                        Runtime::BufferManagerPtr) {
        PipelinePlan plan;
        auto orderJoinHandler = createOrderPipeline(plan, tables);
        auto custJoinHandler = createCustPipeline(plan, tables, orderJoinHandler);
        auto lineItemJoinHandler = createLineItemPipeline(plan, tables, custJoinHandler);
        auto suppJoinHandler = createSupplierPipeline(plan, tables, lineItemJoinHandler);
        auto nationJoinHandler = createNationPipeline(plan, tables, suppJoinHandler);
        createRegionPipeline(plan, tables, nationJoinHandler);

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
    static std::shared_ptr<BatchJoinHandler>
    createCustPipeline(PipelinePlan& plan,
                       std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                       const Runtime::Execution::OperatorHandlerPtr& orderJoinHandler) {
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

        /*
         * 3. Build hash table with o_orderkey as key
         */
        auto readO_orderkey = std::make_shared<ReadFieldExpression>("o_orderkey");
        auto orderKeyJoinBuild =
            std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                        std::vector<Expressions::ExpressionPtr>{readO_orderkey},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>(),
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        customersJoinProbe->setChild(orderKeyJoinBuild);

        // Create the customer pipeline
        auto orderCustomersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        orderCustomersJoinBuild->setRootOperator(customerScan);
        auto orderCustJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {orderJoinHandler, orderCustJoinHandler};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderCustomersJoinBuild, pipeline2Context);
        return orderCustJoinHandler;
    }

    // LineItem Pipeline: Scan->JoinProbe(with order-customer)->JoinBuild
    static std::shared_ptr<BatchJoinHandler>
    createLineItemPipeline(PipelinePlan& plan,
                           std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                           const Runtime::Execution::OperatorHandlerPtr& orderCustJoinHandler) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        /*
         * 1. Scan
         */
        auto& lineItems = tables[TPCHTable::LineItem];
        auto lineItemsMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineItems->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> lineItemsProjection = {"l_suppkey", "l_orderkey"};
        auto lineItemScan = std::make_shared<Operators::Scan>(std::move(lineItemsMemoryProviderPtr), lineItemsProjection);

        /*
         * 2. JoinProbe (with order-customer)
         */
        std::vector<ExpressionPtr> lineItemOrderKey = {std::make_shared<ReadFieldExpression>("l_orderkey")};
        auto customersJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                   lineItemOrderKey,
                                                                   std::vector<PhysicalTypePtr>{integerType},
                                                                   std::vector<Record::RecordFieldIdentifier>(),
                                                                   std::vector<PhysicalTypePtr>(),
                                                                   std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        lineItemScan->setChild(customersJoinProbe);

        /*
         * 3. Build hash table with o_orderkey as key
         */
        auto readL_suppkey = std::make_shared<ReadFieldExpression>("l_suppkey");
        auto lineItemSuppKeyJoinBuild =
            std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                        std::vector<Expressions::ExpressionPtr>{readL_suppkey},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>(),
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        customersJoinProbe->setChild(lineItemSuppKeyJoinBuild);

        // Create the lineitem pipeline
        auto orderCustomersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        orderCustomersJoinBuild->setRootOperator(lineItemScan);
        auto orderCustLitemJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {orderCustJoinHandler, orderCustLitemJoinHandler};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderCustomersJoinBuild, pipeline2Context);
        return orderCustLitemJoinHandler;
    }

    // Supplier Pipeline: Scan->JoinProbe(with order-customer-lineitem)->JoinBuild
    static std::shared_ptr<BatchJoinHandler>
    createSupplierPipeline(PipelinePlan& plan,
                           std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                           const Runtime::Execution::OperatorHandlerPtr& orderCustLitemJoinHandler) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        /*
         * 1. Scan
         */
        auto& suppliers = tables[TPCHTable::Supplier];
        auto suppliersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(suppliers->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> suppliersProjection = {"s_nationkey", "s_suppkey"};
        auto suppliersScan = std::make_shared<Operators::Scan>(std::move(suppliersMemoryProviderPtr), suppliersProjection);

        /*
         * 2. JoinProbe (with order-customer-lineitem)
         */
        std::vector<ExpressionPtr> supplierSuppKey = {std::make_shared<ReadFieldExpression>("s_suppkey")};
        auto customersJoinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                   supplierSuppKey,
                                                                   std::vector<PhysicalTypePtr>{integerType},
                                                                   std::vector<Record::RecordFieldIdentifier>(),
                                                                   std::vector<PhysicalTypePtr>(),
                                                                   std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        suppliersScan->setChild(customersJoinProbe);

        /*
         * 3. Build hash table with s_nationkey as key
         */
        auto readS_nationkey = std::make_shared<ReadFieldExpression>("s_nationkey");
        auto lineItemSuppKeyJoinBuild =
            std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                        std::vector<Expressions::ExpressionPtr>{readS_nationkey},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>(),
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        customersJoinProbe->setChild(lineItemSuppKeyJoinBuild);

        // Create the supplier pipeline
        auto orderCustomersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        orderCustomersJoinBuild->setRootOperator(suppliersScan);
        auto orderCustLitemSuppJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {orderCustLitemJoinHandler, orderCustLitemSuppJoinHandler};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderCustomersJoinBuild, pipeline2Context);
        return orderCustLitemSuppJoinHandler;
    }

    // Nation Pipeline: Scan->JoinProbe(with order-customer-lineitem-supplier)->JoinBuild
    static std::shared_ptr<BatchJoinHandler>
    createNationPipeline(PipelinePlan& plan,
                         std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                         const Runtime::Execution::OperatorHandlerPtr& orderCustLitemSuppJoinHandler) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        /*
         * 1. Scan
         */
        auto& nations = tables[TPCHTable::Nation];
        auto nationsMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(nations->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> nationProjection = {"n_nationkey", "n_regionkey"};
        auto nationsScan = std::make_shared<Operators::Scan>(std::move(nationsMemoryProviderPtr), nationProjection);

        /*
         * 2. JoinProbe (with order-customer-lineitem-supplier)
         */
        std::vector<ExpressionPtr> nationNationKey = {std::make_shared<ReadFieldExpression>("n_nationkey")};
        auto joinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                                   nationNationKey,
                                                                   std::vector<PhysicalTypePtr>{integerType},
                                                                   std::vector<Record::RecordFieldIdentifier>(),
                                                                   std::vector<PhysicalTypePtr>(),
                                                                   std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        nationsScan->setChild(joinProbe);

        /*
         * 3. Build hash table with n_nationkey as key
         */
        auto readN_nationkey = std::make_shared<ReadFieldExpression>("n_nationkey");
        auto lineItemSuppKeyJoinBuild =
            std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                        std::vector<Expressions::ExpressionPtr>{readN_nationkey},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>(),
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        joinProbe->setChild(lineItemSuppKeyJoinBuild);

        // Create the supplier pipeline
        auto orderCustLItemSuppNatJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        orderCustLItemSuppNatJoinBuild->setRootOperator(nationsScan);
        auto orderCustLitemSuppNatJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {orderCustLitemSuppJoinHandler, orderCustLitemSuppNatJoinHandler};
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderCustLItemSuppNatJoinBuild, pipeline2Context);
        return orderCustLitemSuppNatJoinHandler;
    }

    // Region Pipeline: Scan->Selection->JoinProbe(with order-customer-lineitem-supplier-nation)->JoinBuild
    static void createRegionPipeline(PipelinePlan& plan,
                         std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                         const Runtime::Execution::OperatorHandlerPtr& orderCustLitemSuppNatJoinHandler) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /*
         * 1. Scan
         */
        auto& regions = tables[TPCHTable::Region];
        auto regionsMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(regions->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> regionsProjection = {"r_regionkey", "r_name"};
        auto regionsScan = std::make_shared<Operators::Scan>(std::move(regionsMemoryProviderPtr), regionsProjection);

        /*
         * 2. Selection: r_name = 'ASIA' (currently modeled as 1)
         */
        auto asia = std::make_shared<ConstantInt32ValueExpression>(1);
        auto readR_name = std::make_shared<ReadFieldExpression>("r_name");
        auto equalsExpression = std::make_shared<EqualsExpression>(readR_name, asia);
        auto selection = std::make_shared<Selection>(equalsExpression);
        regionsScan->setChild(selection);

        /*
         * 3. JoinProbe (with order-customer-lineitem-supplier-nation)
         */
        std::vector<ExpressionPtr> regionRegionKey = {std::make_shared<ReadFieldExpression>("r_regionkey")};
        auto joinProbe = std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                                          regionRegionKey,
                                                          std::vector<PhysicalTypePtr>{integerType},
                                                          std::vector<Record::RecordFieldIdentifier>(),
                                                          std::vector<PhysicalTypePtr>(),
                                                          std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        selection->setChild(joinProbe);

        /*
         * 4. Aggregate
         */
        //  sum(l_extendedprice * (1 - l_discount)) as revenue,
        auto l_orderkey = std::make_shared<ReadFieldExpression>("l_orderkey");
        std::vector<ExpressionPtr> lineitemProbeKeys = {l_orderkey};

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

        // create lineitems_ordersJoinBuild pipeline
        auto lineitems_ordersJoinBuild = std::make_shared<PhysicalOperatorPipeline>();
        lineitems_ordersJoinBuild->setRootOperator(regionsScan);
        auto aggHandler = std::make_shared<Operators::BatchKeyedAggregationHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers2 = {orderCustLitemSuppNatJoinHandler, aggHandler};
        auto pipeline3Context = std::make_shared<MockedPipelineExecutionContext>(handlers2);
        plan.appendPipeline(lineitems_ordersJoinBuild, pipeline3Context);
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
