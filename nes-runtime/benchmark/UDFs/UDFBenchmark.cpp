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
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Table.hpp"
#include "UDF/FilterMap.hpp"
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
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
#include <UDF/CrimeIndexMap.hpp>
#include <UDF/DistanceMap.hpp>
#include <UDF/FilterMap.hpp>
#include <UDF/SimpleMap.hpp>
#include <UDF/UppercaseMap.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace NES::Runtime::Execution {

struct BenchmarkOptions {
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
        options.setOptimize(true);
        options.setDumpToFile(true);
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
            sumExecution += executionTimeTimer.getPrintTime();
            NES_INFO2("Run {} warmup time {}", i, executionTimeTimer.getPrintTime());
        }

        auto avgCompilationTime = (sumCompilation / (double) bmOptions.compileIterations);
        auto avgExecutionTime = (sumExecution / (double) bmOptions.executionBenchmark);
        NES_INFO2("Final {} compilation time {}, execution time {} ", bmOptions.compiler, avgCompilationTime, avgExecutionTime);

        std::ofstream file(bmOptions.identifier, std::ios::app);
        if (file.is_open()) {
            // Append the file: query, compiler, compiletime, execution time
            file << fmt::format("{},{},{},{}", bmOptions.query, bmOptions.compiler, avgCompilationTime, avgExecutionTime)
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
    BenchmarkOptions bmOptions;
};

class SimpleMapRunner : public BenchmarkRunner {
  public:
    SimpleMapRunner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    std::unique_ptr<Table> table;
    void setup() override {
        auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)->addField("value", BasicType::INT32);
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t integers = (1024 * 1024 * 5) / sizeof(int32_t);
        for (int32_t i = 0; i < integers; i++) {
            tb.append(std::make_tuple((int32_t) i));
        }

        table = tb.finishTable();
        plan = NES::Runtime::Execution::SimpleMap::getPipelinePlan(table, bm);
    }
    void compileQuery(Timer<>& compileTimeTimer) override {

        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;

        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);
        executionTimeTimer.start();
        aggExecutablePipeline->setup(*pipeline1.ctx);
        for (auto& chunk : table->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
};

class DistanceMapRunner : public BenchmarkRunner {
  public:
    DistanceMapRunner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    std::unique_ptr<Table> table;
    void setup() override {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("lat1", BasicType::FLOAT64)
                          ->addField("long1", BasicType::FLOAT64)
                          ->addField("lat2", BasicType::FLOAT64)
                          ->addField("long2", BasicType::FLOAT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t integers = (1024 * 1024) / sizeof(int32_t);
        for (int32_t i = 0; i < integers; i++) {
            tb.append(std::make_tuple(53.551086, 9.993682, 52.520008, 13.404954));
        }

        table = tb.finishTable();
        plan = NES::Runtime::Execution::DistanceMap::getPipelinePlan(table, bm);
    }
    void compileQuery(Timer<>& compileTimeTimer) override {

        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;

        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);
        executionTimeTimer.start();
        aggExecutablePipeline->setup(*pipeline1.ctx);
        for (auto& chunk : table->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
};

class UppercaseMapRunner : public BenchmarkRunner {
  public:
    UppercaseMapRunner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<Table> table;
    void setup() override {
        auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)->addField("lat1", BasicType::TEXT);
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t integers = (1024 * 10) / sizeof(int32_t);
        for (int32_t i = 0; i < integers; i++) {
            auto text = TextValue::create("hello");
            tb.append(std::make_tuple(text));
            text->~TextValue();
        }

        table = tb.finishTable();
        plan = NES::Runtime::Execution::UppercaseMap::getPipelinePlan(table, bm);
    }
    void compileQuery(Timer<>& compileTimeTimer) override {

        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;

        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);
        executionTimeTimer.start();
        aggExecutablePipeline->setup(*pipeline1.ctx);
        for (auto& chunk : table->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
};

class FilterUDFRunner : public BenchmarkRunner {
  public:
    FilterUDFRunner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    std::unique_ptr<Table> table;
    void setup() override {
        auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)->addField("value", BasicType::INT32);
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t integers = (1024 * 100) / sizeof(int32_t);
        for (int32_t i = 0; i < integers; i++) {
            tb.append(std::make_tuple((int32_t) i));
        }

        table = tb.finishTable();
        plan = NES::Runtime::Execution::FilterUDF::getPipelinePlan(table, bm);
    }
    void compileQuery(Timer<>& compileTimeTimer) override {

        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;

        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);
        executionTimeTimer.start();
        aggExecutablePipeline->setup(*pipeline1.ctx);
        for (auto& chunk : table->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
};

class CrimeIndexUDFRunner : public BenchmarkRunner {
  public:
    CrimeIndexUDFRunner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    std::unique_ptr<Table> table;
    void setup() override {

        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("total_population", BasicType::INT64)
                          ->addField("total_adult_population", BasicType::INT64)
                          ->addField("number_of_robberies", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t integers = (1024 * 100) / sizeof(int32_t);
        for (int32_t i = 0; i < integers; i++) {
            auto total_population = i % 100 == 0 ? 100000 : 255;
            total_population = (total_population * (i%42)) / 2;
            auto total_adult_population = (int64_t)(total_population * 0.7);
            auto number_of_robberies = (int64_t)(total_population * 0.01);
            tb.append(std::make_tuple((int64_t) total_population, total_adult_population, number_of_robberies));
        }

        table = tb.finishTable();
        plan = NES::Runtime::Execution::CrimeIndexMap::getPipelinePlan(table, bm);
    }
    void compileQuery(Timer<>& compileTimeTimer) override {

        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;

        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);
        executionTimeTimer.start();
        aggExecutablePipeline->setup(*pipeline1.ctx);
        for (auto& chunk : table->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
};

}// namespace NES::Runtime::Execution
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
    NES::Runtime::Execution::BenchmarkOptions options = {.compiler = commandLineParams["compiler"],
                                                         .compileIterations = compileIterations,
                                                         .executionWarmup = executionWarmup,
                                                         .executionBenchmark = executionBenchmark,
                                                         .identifier = commandLineParams["identifier"],
                                                         .query = commandLineParams["query"]};

    auto query = commandLineParams["query"];
    if (query == "simpleMap") {
        NES::Runtime::Execution::SimpleMapRunner(options).run();
    } else if (query == "distance") {
        NES::Runtime::Execution::DistanceMapRunner(options).run();
    } else if (query == "uppercase") {
        NES::Runtime::Execution::UppercaseMapRunner(options).run();
    } else if (query == "filter") {
        NES::Runtime::Execution::FilterUDFRunner(options).run();
    } else if (query == "crime") {
        NES::Runtime::Execution::CrimeIndexUDFRunner(options).run();
    }
    return 0;
}
