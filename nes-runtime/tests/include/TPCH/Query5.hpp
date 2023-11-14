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
        auto customerJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                        std::vector<Expressions::ExpressionPtr>{readCCustKey},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>(),
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
        std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
        std::vector<IR::Types::StampPtr> valueStamps = {};
        std::vector<ExpressionPtr> ordersProbeKeys = {std::make_shared<ReadFieldExpression>("o_custkey")};

        auto orderJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             ordersProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             std::vector<Record::RecordFieldIdentifier>(),
                                             std::vector<PhysicalTypePtr>(),
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
        auto order_customersJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        orderJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>{integerType, integerType},
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

        /**
        * LineItem pipeline: Scan(LineItem) -> Probe(w/Order) -> Build
        */
        // Scan the LineItem table
        auto lineItemMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineItemTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> lineItemProjection = {"l_orderkey", "l_suppkey"};
        auto lineItemScanOperator = std::make_shared<Operators::Scan>(std::move(lineItemMemoryProviderPtr), lineItemProjection);

        // Probe with Order
        std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
        std::vector<IR::Types::StampPtr> valueStamps = {};
        std::vector<ExpressionPtr> lineItemProbeKeys = {std::make_shared<ReadFieldExpression>("l_orderkey")};

        auto lineItemJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             lineItemProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             std::vector<Record::RecordFieldIdentifier>(),
                                             std::vector<PhysicalTypePtr>(),
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        lineItemScanOperator->setChild(lineItemJoinProbeOperator);

        // Build on LineItem
        std::vector<ExpressionPtr> lineItemJoinBuildKeys = {std::make_shared<ReadFieldExpression>("l_suppkey")};
        auto lineItemJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        lineItemJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>{integerType, integerType},
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

    // TODO:  This join is not yet added: "AND c_nationkey = s_nationkey"
    static std::shared_ptr<BatchJoinHandler>
    createSupplierPipeline(PipelinePlan& plan,
                           std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                           const Runtime::Execution::OperatorHandlerPtr& lineItemJoinHandler) {
        auto& supplierTable = tables[TPCHTable::Supplier];

        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());

        /**
        * Supplier pipeline: Scan(Supplier) -> Probe(w/LineItem) -> Build
        */
        // Scan the Supplier table
        auto supplierMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(supplierTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> supplierProjection = {"s_nationkey", "s_suppkey"};
        auto supplierScanOperator = std::make_shared<Operators::Scan>(std::move(supplierMemoryProviderPtr), supplierProjection);

        // Probe with LineItem
        std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
        std::vector<IR::Types::StampPtr> valueStamps = {};
        std::vector<ExpressionPtr> supplierProbeKeys = {std::make_shared<ReadFieldExpression>("s_suppkey")};

        auto supplierJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             supplierProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             std::vector<Record::RecordFieldIdentifier>(),
                                             std::vector<PhysicalTypePtr>(),
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        supplierScanOperator->setChild(supplierJoinProbeOperator);

        // Build on Supplier
        std::vector<ExpressionPtr> supplierJoinBuildKeys = {std::make_shared<ReadFieldExpression>("s_nationkey")};
        auto supplierJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        supplierJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>{integerType, integerType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        supplierJoinProbeOperator->setChild(supplierJoinBuildOperator);

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

        /**
        * Nation pipeline: Scan(Nation) -> Probe(w/Supplier) -> Build
        */
        // Scan the Nation table
        auto nationMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(nationTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> nationProjection = {"n_nationkey", "n_regionkey"};
        auto nationScanOperator = std::make_shared<Operators::Scan>(std::move(nationMemoryProviderPtr), nationProjection);

        // Probe with Supplier
        std::vector<IR::Types::StampPtr> keyStamps = {IR::Types::StampFactory::createInt64Stamp()};
        std::vector<IR::Types::StampPtr> valueStamps = {};
        std::vector<ExpressionPtr> nationProbeKeys = {std::make_shared<ReadFieldExpression>("n_nationkey")};

        auto nationJoinProbeOperator =
            std::make_shared<BatchJoinProbe>(0 /*handler index*/,
                                             nationProbeKeys,
                                             std::vector<PhysicalTypePtr>{integerType},
                                             std::vector<Record::RecordFieldIdentifier>(),
                                             std::vector<PhysicalTypePtr>(),
                                             std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        nationScanOperator->setChild(nationJoinProbeOperator);

        // Build on Nation
        std::vector<ExpressionPtr> nationJoinBuildKeys = {std::make_shared<ReadFieldExpression>("n_regionkey")};
        auto nationJoinBuildOperator =
            std::make_shared<Operators::BatchJoinBuild>(1 /*handler index*/,
                                                        nationJoinBuildKeys,
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::vector<Expressions::ExpressionPtr>(),
                                                        std::vector<PhysicalTypePtr>{integerType, integerType},
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
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
