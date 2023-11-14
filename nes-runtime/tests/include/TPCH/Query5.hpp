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

        return plan;
    }

    static Runtime::Execution::OperatorHandlerPtr
    createCustomerPipeline(PipelinePlan& plan, std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables) {
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
        auto& customerTable = tables[TPCHTable::Customer];

        auto c_scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(customerTable->getLayout()));
        std::vector<Nautilus::Record::RecordFieldIdentifier> customersProjection = {"c_custkey", "c_nationkey"};
        auto customersScanOperator = std::make_shared<Operators::Scan>(std::move(c_scanMemoryProviderPtr), customersProjection);

        // build ht for first join
        auto readCCustKey = std::make_shared<ReadFieldExpression>("c_custkey");
        auto customerJoinBuildOperator = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                                  std::vector<Expressions::ExpressionPtr>{readCCustKey},
                                                                  std::vector<PhysicalTypePtr>{integerType},
                                                                  std::vector<Expressions::ExpressionPtr>(),
                                                                  std::vector<PhysicalTypePtr>(),
                                                                  std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        customersScanOperator->setChild(customerJoinBuildOperator);

        // create the customer pipeline
        auto customerPipeline = std::make_shared<PhysicalOperatorPipeline>();
        customerPipeline->setRootOperator(customersScanOperator);
        std::vector<Runtime::Execution::OperatorHandlerPtr> customerJoinHandler = {std::make_shared<Operators::BatchJoinHandler>()};
        auto customerPipelineContex = std::make_shared<MockedPipelineExecutionContext>(customerJoinHandler);
        plan.appendPipeline(customerPipeline, customerPipelineContex);
        return customerJoinHandler[0];
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_QUERY5_HPP_
