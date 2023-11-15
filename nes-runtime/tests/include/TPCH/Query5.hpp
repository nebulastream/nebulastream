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
        auto customerJoinHandler = createCustomerPipeline(plan, tables);
        auto orderJoinHandler = createOrderPipeline(plan, tables, customerJoinHandler);
        auto lineItemJoinHandler = createLineItemPipeline(plan, tables, orderJoinHandler);
        auto supplierJoinHandler = createSupplierPipeline(plan, tables, lineItemJoinHandler);
        auto nationJoinHandler = createNationPipeline(plan, tables, supplierJoinHandler);
        createRegionPipeline(plan, tables, nationJoinHandler);

        return plan;
    }

    static Runtime::Execution::OperatorHandlerPtr
    createCustomerPipeline(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        auto& customerTable = tables[TPCHTable::Customer];

        auto c_scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(customerTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> customersProjection = {"c_custkey", "c_nationkey"};
        auto customersScanOperator = std::make_shared<Operators::Scan>(std::move(c_scanMemoryProviderPtr), customersProjection);

        // build ht for first join
        auto readCCustKey = std::make_shared<ReadFieldExpression>("c_custkey");
        std::vector<ExpressionPtr> customerJoinBuildValues = {std::make_shared<ReadFieldExpression>("c_nationkey")};
        auto customerJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                        std::vector<Expressions::ExpressionPtr>{readCCustKey},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        customerJoinBuildValues,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        customersScanOperator->setChild(customerJoinBuildOperator);

        // create the customer pipeline
        auto customerPipeline = std::make_shared<PhysicalOperatorPipeline>();
        customerPipeline->setRootOperator(customersScanOperator);
        std::vector<Runtime::Execution::OperatorHandlerPtr> customerJoinHandler = {
            std::make_shared<Operators::BatchJoinHandler>()};
        auto customerPipelineContex = std::make_shared<MockedPipelineExecutionContext>(customerJoinHandler);
        plan.appendPipeline(customerPipeline, customerPipelineContex);
        return customerJoinHandler[0];
    }

    static std::shared_ptr<BatchJoinHandler>
    createOrderPipeline(PipelinePlan& plan,
                        std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                        const Runtime::Execution::OperatorHandlerPtr& customerJoinHandler) {
        auto& orderTable = tables[TPCHTable::Orders];

        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        /**
        * Order pipeline: Scan(Order) -> Probe(w/Customer) -> Selection -> Build
        */
        // Scan the Order table
        auto ordersMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(orderTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> ordersProjection = {"o_orderkey", "o_orderdate", "o_custkey"};
        auto orderScanOperator = std::make_shared<Operators::Scan>(std::move(ordersMemoryProviderPtr), ordersProjection);

        // Probe with Customer
        std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};
        std::vector<Nautilus::Record::RecordFieldIdentifier> orderProbeFieldIdentifier = {"c_nationkey"};

        auto orderJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             ordersProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             orderProbeFieldIdentifier,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        orderScanOperator->setChild(orderJoinProbeOperator);

        auto const_1994_01_01 = std::make_shared<ConstantInt32ValueExpression>(19940101);
        auto const_1995_01_01 = std::make_shared<ConstantInt32ValueExpression>(19950101);
        auto readOrderDate = std::make_shared<ReadFieldExpression>("o_orderdate");

        auto lessThanExpression1 = std::make_shared<GreaterEqualsExpression>(const_1994_01_01, readOrderDate);
        auto lessThanExpression2 = std::make_shared<LessThanExpression>(readOrderDate, const_1995_01_01);
        auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);

        auto orderDateSelectionOperator = std::make_shared<Selection>(andExpression);
        orderJoinProbeOperator->setChild(orderDateSelectionOperator);

        // Build on Order
        std::vector<ExpressionPtr> orderJoinBuildKeys = {std::make_shared<ReadFieldExpression>("o_orderkey")};
        std::vector<ExpressionPtr> orderJoinBuildValues = {std::make_shared<ReadFieldExpression>("c_nationkey")};
        auto order_customersJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        orderJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        orderJoinBuildValues,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        orderDateSelectionOperator->setChild(order_customersJoinBuildOperator);

        // create order_customersJoinBuild pipeline
        auto orderPipeline = std::make_shared<PhysicalOperatorPipeline>();
        orderPipeline->setRootOperator(orderScanOperator);
        auto orderJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {customerJoinHandler, orderJoinHandler};
        auto orderPipelineContext = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(orderPipeline, orderPipelineContext);
        return orderJoinHandler;
    }

    static std::shared_ptr<BatchJoinHandler>
    createLineItemPipeline(PipelinePlan& plan,
                           std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                           const Runtime::Execution::OperatorHandlerPtr& orderJoinHandler) {
        auto& lineItemTable = tables[TPCHTable::LineItem];

        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /**
        * LineItem pipeline: Scan(LineItem) -> Probe(w/Order) -> Build
        */
        // Scan the LineItem table
        auto lineItemMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineItemTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> lineItemProjection = {"l_orderkey",
                                                                                   "l_suppkey",
                                                                                   "l_extendedprice",
                                                                                   "l_discount"};
        auto lineItemScanOperator = std::make_shared<Operators::Scan>(std::move(lineItemMemoryProviderPtr), lineItemProjection);

        // Probe with Order
        std::vector<ExpressionPtr> lineItemProbeKeys = {std::make_shared<ReadFieldExpression>("l_orderkey")};
        std::vector<Nautilus::Record::RecordFieldIdentifier> lineItemProbeFieldIdentifier = {"l_extendedprice",
                                                                                             "l_discount",
                                                                                             "c_nationkey"};
        auto lineItemJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             lineItemProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             lineItemProbeFieldIdentifier,
                                             std::vector<PhysicalTypePtr>{floatType, floatType, integerType},
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        lineItemScanOperator->setChild(lineItemJoinProbeOperator);

        // Build on LineItem
        std::vector<ExpressionPtr> lineItemJoinBuildKeys = {std::make_shared<ReadFieldExpression>("l_suppkey")};
        std::vector<ExpressionPtr> lineItemJoinBuildValues = {std::make_shared<ReadFieldExpression>("l_extendedprice"),
                                                              std::make_shared<ReadFieldExpression>("l_discount"),
                                                              std::make_shared<ReadFieldExpression>("c_nationkey")};
        auto lineItemJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        lineItemJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        lineItemJoinBuildValues,
                                                        std::vector<PhysicalTypePtr>{floatType, floatType, integerType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        lineItemJoinProbeOperator->setChild(lineItemJoinBuildOperator);

        // Create the LineItem pipeline
        auto lineItemPipeline = std::make_shared<PhysicalOperatorPipeline>();
        lineItemPipeline->setRootOperator(lineItemScanOperator);
        auto lineItemJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {orderJoinHandler, lineItemJoinHandler};
        auto lineItemPipelineContext = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(lineItemPipeline, lineItemPipelineContext);

        return lineItemJoinHandler;
    }

    static std::shared_ptr<BatchJoinHandler>
    createSupplierPipeline(PipelinePlan& plan,
                           std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                           const Runtime::Execution::OperatorHandlerPtr& lineItemJoinHandler) {
        auto& supplierTable = tables[TPCHTable::Supplier];

        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /**
        * Supplier pipeline: Scan(Supplier) -> Selection (c_nationkey == s_nationkey) --> Probe(w/LineItem) -> Build
        */
        // Scan the Supplier table
        auto supplierMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(supplierTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> supplierProjection = {"s_nationkey", "s_suppkey"};
        auto supplierScanOperator = std::make_shared<Operators::Scan>(std::move(supplierMemoryProviderPtr), supplierProjection);

        // Probe with LineItem
        std::vector<ExpressionPtr> supplierProbeKeys = {std::make_shared<ReadFieldExpression>("s_suppkey")};
        std::vector<Nautilus::Record::RecordFieldIdentifier> supplierProbeFieldIdentifier = {"l_extendedprice", "l_discount", "c_nationkey"};

        auto supplierJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             supplierProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             supplierProbeFieldIdentifier,
                                             std::vector<PhysicalTypePtr>{floatType, floatType, integerType},
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        supplierScanOperator->setChild(supplierJoinProbeOperator);

        // Selection c_nationkey == s_nationkey
        auto readsNationKey = std::make_shared<ReadFieldExpression>("s_nationkey");
        auto readCNationKey = std::make_shared<ReadFieldExpression>("c_nationkey");
        auto equalsExpression = std::make_shared<EqualsExpression>(readsNationKey, readCNationKey);
        auto suppliySelectionOperator = std::make_shared<Selection>(equalsExpression);
        supplierJoinProbeOperator->setChild(suppliySelectionOperator);

        // Build on Supplier
        std::vector<ExpressionPtr> supplierJoinBuildKeys = {std::make_shared<ReadFieldExpression>("s_nationkey")};
        std::vector<ExpressionPtr> supplierJoinBuildValues = {std::make_shared<ReadFieldExpression>("l_extendedprice"),
                                                              std::make_shared<ReadFieldExpression>("l_discount")};
        auto supplierJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        supplierJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        supplierJoinBuildValues,
                                                        std::vector<PhysicalTypePtr>{floatType, floatType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        suppliySelectionOperator->setChild(supplierJoinBuildOperator);

        // Create the Supplier pipeline
        auto supplierPipeline = std::make_shared<PhysicalOperatorPipeline>();
        supplierPipeline->setRootOperator(supplierScanOperator);
        auto supplierJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {lineItemJoinHandler, supplierJoinHandler};
        auto supplierPipelineContext = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(supplierPipeline, supplierPipelineContext);

        return supplierJoinHandler;
    }

    static std::shared_ptr<BatchJoinHandler>
    createNationPipeline(PipelinePlan& plan,
                         std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                         const Runtime::Execution::OperatorHandlerPtr& supplierJoinHandler) {
        auto& nationTable = tables[TPCHTable::Nation];

        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /**
        * Nation pipeline: Scan(Nation) -> Probe(w/Supplier) -> Build
        */
        // Scan the Nation table
        auto nationMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(nationTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> nationProjection = {"n_nationkey", "n_regionkey", "n_name"};
        auto nationScanOperator = std::make_shared<Operators::Scan>(std::move(nationMemoryProviderPtr), nationProjection);

        // Probe with Supplier
        std::vector<ExpressionPtr> nationProbeKeys = {std::make_shared<ReadFieldExpression>("n_nationkey")};
        std::vector<Nautilus::Record::RecordFieldIdentifier> nationProbeFieldIdentifier = {"l_extendedprice", "l_discount"};
        auto nationJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             nationProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             nationProbeFieldIdentifier,
                                             std::vector<PhysicalTypePtr>{floatType, floatType},
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        nationScanOperator->setChild(nationJoinProbeOperator);

        // Build on Nation
        std::vector<ExpressionPtr> nationJoinBuildKeys = {std::make_shared<ReadFieldExpression>("n_regionkey")};
        std::vector<ExpressionPtr> nationJoinBuildValues = {std::make_shared<ReadFieldExpression>("n_name"),
                                                            std::make_shared<ReadFieldExpression>("l_extendedprice"),
                                                            std::make_shared<ReadFieldExpression>("l_discount")};
        auto nationJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        nationJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        nationJoinBuildValues,
                                                        std::vector<PhysicalTypePtr>{integerType, floatType, floatType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        nationJoinProbeOperator->setChild(nationJoinBuildOperator);

        // Create the Nation pipeline
        auto nationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        nationPipeline->setRootOperator(nationScanOperator);
        auto nationJoinHandler = std::make_shared<Operators::BatchJoinHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {supplierJoinHandler, nationJoinHandler};
        auto nationPipelineContext = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(nationPipeline, nationPipelineContext);

        return nationJoinHandler;
    }

    static void createRegionPipeline(PipelinePlan& plan,
                                     std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                     const Runtime::Execution::OperatorHandlerPtr& nationJoinHandler) {
        auto& regionTable = tables[TPCHTable::Region];

        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        /**
        * Region pipeline: Scan(Region) -> Probe(w/Nation) -> Selection --> Build
        */
        // Scan the Region table
        auto regionMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(regionTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> regionProjection = {"r_name", "r_regionkey"};
        auto regionScanOperator = std::make_shared<Operators::Scan>(std::move(regionMemoryProviderPtr), regionProjection);

        // Selection r_name = 'ASIA' -> currently modeled as 1 (as in Query 3)
        auto asia = std::make_shared<ConstantInt32ValueExpression>(1);
        auto readRName = std::make_shared<ReadFieldExpression>("r_name");
        auto equalsExpression = std::make_shared<EqualsExpression>(readRName, asia);
        auto regionSelectionOperator = std::make_shared<Selection>(equalsExpression);
        regionScanOperator->setChild(regionSelectionOperator);

        // Probe with Nation
        std::vector<ExpressionPtr> regionProbeKeys = {std::make_shared<ReadFieldExpression>("r_regionkey")};
        std::vector<Record::RecordFieldIdentifier> regionProbeFieldNames = {"n_name", "l_extendedprice", "l_discount"};

        auto regionJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             regionProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             regionProbeFieldNames,
                                             std::vector<PhysicalTypePtr>{integerType, floatType, floatType},
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        regionSelectionOperator->setChild(regionJoinProbeOperator);

        // Aggregation: sum(l_extendedprice * (1 - l_discount)) as revenue
        auto lineItemExtendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
        auto lineItemdiscountField = std::make_shared<ReadFieldExpression>("l_discount");
        auto oneConst = std::make_shared<ConstantFloatValueExpression>(1.0f);
        auto subExpression = std::make_shared<SubExpression>(oneConst, lineItemdiscountField);
        auto revenueExpression = std::make_shared<MulExpression>(lineItemExtendedpriceField, subExpression);
        auto sumRevenue =
            std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType, revenueExpression, "sum_revenue");
        auto readNationName = std::make_shared<ReadFieldExpression>("n_name");
        std::vector<Expressions::ExpressionPtr> keyFields = {readNationName};
        std::vector<Expressions::ExpressionPtr> aggregationExpressions = {revenueExpression};
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {sumRevenue};

        PhysicalTypePtr smallType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt8());

        auto aggregation = std::make_shared<Operators::BatchKeyedAggregation>(
            1 /*handler index*/,
            keyFields,
            std::vector<PhysicalTypePtr>{integerType, integerType, integerType},
            aggregationFunctions,
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        regionJoinProbeOperator->setChild(aggregation);

        // Create Region Pipeline
        auto regionPipeline = std::make_shared<PhysicalOperatorPipeline>();
        regionPipeline->setRootOperator(regionScanOperator);
        auto regionAggregationHandler = std::make_shared<Operators::BatchKeyedAggregationHandler>();
        std::vector<Execution::OperatorHandlerPtr> handlers = {nationJoinHandler, regionAggregationHandler};
        auto regionPipelineContext = std::make_shared<MockedPipelineExecutionContext>(handlers);
        plan.appendPipeline(regionPipeline, regionPipelineContext);
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
