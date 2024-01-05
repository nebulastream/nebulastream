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
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/Query1.hpp>
#include <TPCH/Query3.hpp>
#include <TPCH/Query6.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class BenchmarkRunner {
  public:
    BenchmarkRunner(TPCH_Scale_Factor targetScaleFactor, std::string compiler)
        : targetScaleFactor(targetScaleFactor), compiler(compiler) {

        NES::Logger::setupLogging("BenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, targetScaleFactor).generate();
        options.setOptimize(true);
        options.setDumpToFile(false);
    }
    void run() {
        double sumCompilation = 0;
        double sumExecution = 0;
        for (uint64_t i = 0; i < iterations; i++) {
            Timer compileTimeTimer("Compilation");
            Timer executionTimeTimer("Execution");
            runQuery(compileTimeTimer, executionTimeTimer);
            sumCompilation += compileTimeTimer.getPrintTime();
            sumExecution += executionTimeTimer.getPrintTime();
            NES_INFO("Run {} compilation time {}, execution time {}",
                      i,
                      compileTimeTimer.getPrintTime(),
                      executionTimeTimer.getPrintTime());
        }

        NES_INFO("Final {} compilation time {}, execution time {} ",
                  compiler,
                  (sumCompilation / (double) iterations),
                  (sumExecution / (double) iterations));
    };
    virtual ~BenchmarkRunner() = default;
    virtual void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) = 0;

  protected:
    uint64_t iterations = 10;
    TPCH_Scale_Factor targetScaleFactor = TPCH_Scale_Factor::F1;
    std::string compiler;
    ExecutablePipelineProvider* provider;
    Nautilus::CompilationOptions options;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
};

class Query6Runner : public BenchmarkRunner {
  public:
    Query6Runner(TPCH_Scale_Factor targetScaleFactor, std::string compiler) : BenchmarkRunner(targetScaleFactor, compiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query6::getPipelinePlan(tables, bm);
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        auto emitExecutablePipeline = provider->create(pipeline2.pipeline, options);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        emitExecutablePipeline->setup(*pipeline2.ctx);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();
        executionTimeTimer.start();
        for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute agg");
        auto dummyBuffer = NES::Runtime::TupleBuffer();
        emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
        emitExecutablePipeline->stop(*pipeline2.ctx);
    }
};

class Query1Runner : public BenchmarkRunner {
  public:
    Query1Runner(TPCH_Scale_Factor targetScaleFactor, std::string compiler) : BenchmarkRunner(targetScaleFactor, compiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query1::getPipelinePlan(tables, bm);
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();
        executionTimeTimer.start();
        for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute agg");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
};

class Query3Runner : public BenchmarkRunner {
  public:
    Query3Runner(TPCH_Scale_Factor targetScaleFactor, std::string compiler) : BenchmarkRunner(targetScaleFactor, compiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        auto& customers = tables[TPCHTable::Customer];
        auto& orders = tables[TPCHTable::Orders];
        auto& lineitems = tables[TPCHTable::LineItem];

        auto plan = TPCH_Query3::getPipelinePlan(tables, bm);

        // process query
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto pipeline3 = plan.getPipeline(2);
        compileTimeTimer.start();
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        auto orderCustomersJoinBuildPipeline = provider->create(pipeline2.pipeline, options);
        auto lineitems_ordersJoinBuildPipeline = provider->create(pipeline3.pipeline, options);

        aggExecutablePipeline->setup(*pipeline1.ctx);
        orderCustomersJoinBuildPipeline->setup(*pipeline2.ctx);
        lineitems_ordersJoinBuildPipeline->setup(*pipeline3.ctx);
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (auto& chunk : customers->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        auto joinHandler = pipeline1.ctx->getOperatorHandler<BatchJoinHandler>(0);
        auto numberOfKeys = joinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
        joinHandler->mergeState();

        for (auto& chunk : orders->getChunks()) {
            orderCustomersJoinBuildPipeline->execute(chunk, *pipeline2.ctx, *wc);
        }
        auto joinHandler2 = pipeline2.ctx->getOperatorHandler<BatchJoinHandler>(1);
        auto numberOfKeys2 = joinHandler2->getThreadLocalState(wc->getId())->getNumberOfEntries();
        joinHandler2->mergeState();

        for (auto& chunk : lineitems->getChunks()) {
            lineitems_ordersJoinBuildPipeline->execute(chunk, *pipeline3.ctx, *wc);
        }
        executionTimeTimer.pause();
        auto aggHandler = pipeline3.ctx->getOperatorHandler<BatchKeyedAggregationHandler>(1);
        aggExecutablePipeline->stop(*pipeline1.ctx);
        orderCustomersJoinBuildPipeline->stop(*pipeline2.ctx);
        lineitems_ordersJoinBuildPipeline->stop(*pipeline3.ctx);
    }
};

}// namespace NES::Runtime::Execution

int main(int, char**) {
    NES::TPCH_Scale_Factor targetScaleFactor = NES::TPCH_Scale_Factor::F0_01;
    std::vector<std::string> compilers = {"PipelineCompiler","CPPPipelineCompiler"};
    for (const auto& c : compilers) {
        NES::Runtime::Execution::Query6Runner(targetScaleFactor, c).run();
    }
}