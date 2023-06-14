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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_UDFS_DISTANCE_MAP_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_UDFS_DISTANCE_MAP_HPP_

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
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
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
class DistanceMap {
  public:
    static PipelinePlan getPipelinePlan(std::unique_ptr<NES::Runtime::Table>& table, Runtime::BufferManagerPtr bm) {
        std::string testDataPath = std::string(TEST_DATA_DIRECTORY) + "/JavaUDFTestData/";
       //     "/home/pgrulich/projects/nes/nebulastream/cmake-build-release/nes-runtime/tests/testData/JavaUDFTestData";
        auto schema = table->getLayout()->getSchema();

        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("distance", BasicType::FLOAT64);
        PipelinePlan plan;
        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::RowLayout>(table->getLayout()));
        auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        std::shared_ptr<Operators::ExecutableOperator> mapOperator = std::make_shared<Operators::MapJavaUDF>(0, schema, resultSchema);
        scan->setChild(mapOperator);

        auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        mapOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scan);
        std::unordered_map<std::string, std::vector<char>> byteCodeList = {};
        std::vector<char> serializedInstance = {};
        auto handler = std::make_shared<Operators::JavaUDFOperatorHandler>("stream/nebula/DistanceFunction",
                                                                           "map",
                                                                           "stream/nebula/DistancePojo",
                                                                           "java/lang/Double",
                                                                           byteCodeList,
                                                                           serializedInstance,
                                                                           schema,
                                                                           resultSchema,
                                                                           testDataPath);
        std::vector<OperatorHandlerPtr> handlers = {handler};
        auto pipelineContext = std::make_shared<BlackholePipelineExecutionContext>(handlers);
        plan.appendPipeline(pipeline, pipelineContext);
        return plan;
    }
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_UDFS_DISTANCE_MAP_HPP_
