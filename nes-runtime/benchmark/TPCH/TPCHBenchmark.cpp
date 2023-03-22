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
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <TPCH/Query6.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
#include <Util/Timer.hpp>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <memory>
#include <API/Schema.hpp>
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
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/Query6.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class BenchmarkRunner {
  public:
    TPCH_SCALE_FACTOR targetScaleFactor = TPCH_SCALE_FACTOR::F1;
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
    BenchmarkRunner() {
        NES::Logger::setupLogging("BenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);

        provider = ExecutablePipelineProviderRegistry::getPlugin("PipelineCompiler").get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, targetScaleFactor).generate();
    }
    void run() {
        for (auto i = 0; i < 100; i++) {
            runQuery();
        }
    };
    virtual ~BenchmarkRunner() = default;
    virtual void runQuery() = 0;
};

class Query6Runner : public BenchmarkRunner {
  public:
    void runQuery() override {

        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query6::getPipelinePlan(tables, bm);

        // process table
        NES_INFO2("Process {} chunks", lineitems->getChunks().size());
        Timer timer("Q6");
        timer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline);
        auto emitExecutablePipeline = provider->create(pipeline2.pipeline);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        emitExecutablePipeline->setup(*pipeline2.ctx);
        timer.snapshot("setup");
        for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        timer.snapshot("execute agg");
        auto dummyBuffer = NES::Runtime::TupleBuffer();
        emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
        timer.snapshot("execute emit");

        aggExecutablePipeline->stop(*pipeline1.ctx);
        emitExecutablePipeline->stop(*pipeline2.ctx);
        timer.snapshot("stop");
        timer.pause();
        NES_INFO("Query Runtime:\n" << timer);
    }
};

}// namespace NES::Runtime::Execution

int main(int, char**) {
    auto br = NES::Runtime::Execution::Query6Runner();
    br.run();
}