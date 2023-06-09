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
#include <PipelinePlan.hpp>
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
#include <string>

namespace NES::Runtime::Execution {

struct BenchmarkOptions {
    TPCH_Scale_Factor targetScaleFactor;
    std::string factor;
    std::string compiler;
    uint64_t compileIterations;
    uint64_t executionWarmup;
    uint64_t executionBenchmark;
    std::string identifier;
    std::string query;
};

class BenchmarkRunner {
  public:
    BenchmarkRunner(BenchmarkOptions bmOptions) : bmOptions(bmOptions) {
        NES::Logger::setupLogging("BenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(bmOptions.compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, bmOptions.targetScaleFactor).generate();
        options.setOptimize(true);
        options.setDumpToConsole(false);
        options.setDumpToFile(false);
        options.setOptimizationLevel(3);
    }
    void run() {
        setup();
        double sumCompilation = 0;
        for (uint64_t i = 0; i < bmOptions.compileIterations; i++) {
            Timer compileTimeTimer("Compilation");
            compileQuery(compileTimeTimer);
            sumCompilation += compileTimeTimer.getPrintTime();
            NES_INFO2("Run {} compilation time {}", i, compileTimeTimer.getPrintTime());
        }

        for (uint64_t i = 0; i < bmOptions.executionWarmup; i++) {
            Timer executionTimeTimer("Execution");
            runQuery(executionTimeTimer);
            NES_INFO2("Run {} warmup time {}", i, executionTimeTimer.getPrintTime());
        }

        double sumExecution = 0;
        for (uint64_t i = 0; i < bmOptions.executionBenchmark; i++) {
            Timer executionTimeTimer("Execution");
            runQuery(executionTimeTimer);
            NES_INFO(executionTimeTimer);
            sumExecution += executionTimeTimer.getPrintTime();
            NES_INFO2("Run {} execute time {}", i, executionTimeTimer.getPrintTime());
        }

        auto avgCompilationTime = (sumCompilation / (double) bmOptions.compileIterations);
        auto avgExecutionTime = (sumExecution / (double) bmOptions.executionBenchmark);
        NES_INFO2("Final {} compilation time {}, execution time {} ", bmOptions.compiler, avgCompilationTime, avgExecutionTime);

        std::ofstream file(bmOptions.identifier, std::ios::app);
        if (file.is_open()) {
            // Append the file: query, compiler, compiletime, execution time
            file << fmt::format("{},{},{},{},{}",
                                bmOptions.factor,
                                bmOptions.query,
                                bmOptions.compiler,
                                avgCompilationTime,
                                avgExecutionTime)
                 << std::endl;
            file.flush();
            file.close();
        } else {
            NES_THROW_RUNTIME_ERROR("Failed write of result file");
        }
    };
    virtual ~BenchmarkRunner() = default;
    virtual void setup() = 0;
    virtual void compileQuery(Timer<>& compileTimeTimer) = 0;
    virtual void runQuery(Timer<>& executionTimeTimer) = 0;

  protected:
    ExecutablePipelineProvider* provider;
    Nautilus::CompilationOptions options;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
    BenchmarkOptions bmOptions;
};

class Query6Runner : public BenchmarkRunner {
  public:
    Query6Runner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    void setup() override { plan = TPCH_Query6::getPipelinePlan(tables, bm); }
    void compileQuery(Timer<>& compileTimeTimer) override {
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        //emitExecutablePipeline = provider->create(pipeline2.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        // emitExecutablePipeline->setup(*pipeline2.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto pipeline1 = plan.getPipeline(0);
        // auto pipeline2 = plan.getPipeline(1);
        executionTimeTimer.start();
        aggExecutablePipeline->setup(*pipeline1.ctx);
        //   emitExecutablePipeline->setup(*pipeline2.ctx);
        for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute agg");
        // auto dummyBuffer = NES::Runtime::TupleBuffer();
        // emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
        //  emitExecutablePipeline->stop(*pipeline2.ctx);
    }
};

class Query1Runner : public BenchmarkRunner {
  public:
    Query1Runner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;

    void setup() override { plan = TPCH_Query1::getPipelinePlan(tables, bm); }
    void compileQuery(Timer<>& compileTimeTimer) override {
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto pipeline1 = plan.getPipeline(0);
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
    Query3Runner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> orderCustomersJoinBuildPipeline;
    std::unique_ptr<ExecutablePipelineStage> lineitems_ordersJoinBuildPipeline;
    void setup() override { plan = TPCH_Query3::getPipelinePlan(tables, bm); }
    void compileQuery(Timer<>& compileTimeTimer) override {
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto pipeline3 = plan.getPipeline(2);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        orderCustomersJoinBuildPipeline = provider->create(pipeline2.pipeline, options);
        lineitems_ordersJoinBuildPipeline = provider->create(pipeline3.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        orderCustomersJoinBuildPipeline->setup(*pipeline2.ctx);
        lineitems_ordersJoinBuildPipeline->setup(*pipeline3.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
        orderCustomersJoinBuildPipeline->stop(*pipeline2.ctx);
        lineitems_ordersJoinBuildPipeline->stop(*pipeline3.ctx);
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto& customers = tables[TPCHTable::Customer];
        auto& orders = tables[TPCHTable::Orders];
        auto& lineitems = tables[TPCHTable::LineItem];
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto pipeline3 = plan.getPipeline(2);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        orderCustomersJoinBuildPipeline->setup(*pipeline2.ctx);
        lineitems_ordersJoinBuildPipeline->setup(*pipeline3.ctx);
        // process query
        executionTimeTimer.start();
        for (auto& chunk : customers->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("p1");
        auto joinHandler = pipeline1.ctx->getOperatorHandler<BatchJoinHandler>(0);
        auto numberOfKeys = joinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
        joinHandler->mergeState();
        executionTimeTimer.snapshot("p1 - merge");
        for (auto& chunk : orders->getChunks()) {
            orderCustomersJoinBuildPipeline->execute(chunk, *pipeline2.ctx, *wc);
        }
        executionTimeTimer.snapshot("p2");
        auto joinHandler2 = pipeline2.ctx->getOperatorHandler<BatchJoinHandler>(1);

        joinHandler2->mergeState();
        executionTimeTimer.snapshot("p2 - merge");
        for (auto& chunk : lineitems->getChunks()) {
            lineitems_ordersJoinBuildPipeline->execute(chunk, *pipeline3.ctx, *wc);
        }
        executionTimeTimer.snapshot("p3");
        executionTimeTimer.pause();
        auto aggHandler = pipeline3.ctx->getOperatorHandler<BatchKeyedAggregationHandler>(1);
        aggExecutablePipeline->stop(*pipeline1.ctx);
        orderCustomersJoinBuildPipeline->stop(*pipeline2.ctx);
        lineitems_ordersJoinBuildPipeline->stop(*pipeline3.ctx);
    }
};

}// namespace NES::Runtime::Execution

NES::TPCH_Scale_Factor getScaleFactor(std::map<std::string, std::string> commandLineParams) {
    auto scaleFactor = commandLineParams["scaleFactor"];
    if (scaleFactor == "0.001") {
        return NES::TPCH_Scale_Factor::F0_001;
    } else if (scaleFactor == "0.01") {
        return NES::TPCH_Scale_Factor::F0_01;
    } else if (scaleFactor == "0.1") {
        return NES::TPCH_Scale_Factor::F0_1;
    } else if (scaleFactor == "1") {
        return NES::TPCH_Scale_Factor::F1;
    } else if (scaleFactor == "10") {
        return NES::TPCH_Scale_Factor::F10;
    } else if (scaleFactor == "100") {
        return NES::TPCH_Scale_Factor::F100;
    }
    NES_THROW_RUNTIME_ERROR("Wrong scale factor " + scaleFactor);
}

int main(int argc, char** argv) {

    // Convert the POSIX command line arguments to a map of strings.
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i) {
        const int pos = std::string(argv[i]).find('=');
        const std::string arg{argv[i]};
        commandLineParams.insert({arg.substr(0, pos), arg.substr(pos + 1, arg.length() - 1)});
    }

    auto compileIterations = std::stoul(commandLineParams["compileIterations"]);
    auto executionWarmup = std::stoul(commandLineParams["executionWarmup"]);
    auto executionBenchmark = std::stoul(commandLineParams["executionBenchmark"]);
    NES::Runtime::Execution::BenchmarkOptions options = {.targetScaleFactor = getScaleFactor(commandLineParams),
                                                         .factor = commandLineParams["scaleFactor"],
                                                         .compiler = commandLineParams["compiler"],
                                                         .compileIterations = compileIterations,
                                                         .executionWarmup = executionWarmup,
                                                         .executionBenchmark = executionBenchmark,
                                                         .identifier = commandLineParams["identifier"],
                                                         .query = commandLineParams["query"]};

    auto query = commandLineParams["query"];
    if (query == "q1") {
        NES::Runtime::Execution::Query1Runner(options).run();
    } else if (query == "q3") {
        NES::Runtime::Execution::Query3Runner(options).run();
    } else if (query == "q6") {
        NES::Runtime::Execution::Query6Runner(options).run();
    }
}