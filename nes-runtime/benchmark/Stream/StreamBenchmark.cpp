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
#include <Stream/Nexmark1.hpp>
#include <Stream/Nexmark2.hpp>
#include <Stream/YSB.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
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
static constexpr long PERSON_EVENT_RATIO = 1;
static constexpr long AUCTION_EVENT_RATIO = 4;
static constexpr long BID_EVENT_RATIO = 4;
static constexpr long TOTAL_EVENT_RATIO = PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO + BID_EVENT_RATIO;

static constexpr int MAX_PARALLELISM = 50;

static constexpr long MAX_PERSON_ID = 540000000L;
static constexpr long MAX_AUCTION_ID = 540000000000L;
static constexpr long MAX_BID_ID = 540000000000L;

static constexpr int HOT_SELLER_RATIO = 100;
static constexpr int HOT_AUCTIONS_PROB = 85;
static constexpr int HOT_AUCTION_RATIO = 100;

class BenchmarkRunner {
  public:
    BenchmarkRunner(BenchmarkOptions bmOptions) : bmOptions(bmOptions) {
        NES::Logger::setupLogging("BenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(bmOptions.compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(1024 * 1024, 10000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 1000);
        options.setOptimize(true);
        options.setDumpToFile(false);
        options.setDumpToConsole(true);
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
            file << fmt::format("{},{},{},{}", bmOptions.query, bmOptions.compiler, 0, avgExecutionTime) << std::endl;
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

class AbstractNexmarkBenchmark : public BenchmarkRunner {
  protected:
    AbstractNexmarkBenchmark(BenchmarkOptions options) : BenchmarkRunner(options){};

    std::unique_ptr<Table> table;
    void setup() override {
        auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                          ->addField("auctionId", BasicType::INT64)
                          ->addField("bidderId", BasicType::INT64)
                          ->addField("price", DataTypeFactory::createDecimal(2));

        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t records = (1024 * 1024 * 5) / sizeof(int32_t);
        for (int32_t i = 0; i < records; i++) {

            long auction, bidder;
            long epoch = i / TOTAL_EVENT_RATIO;

            if (rand() % 100 > HOT_AUCTIONS_PROB) {
                auction = (((epoch * AUCTION_EVENT_RATIO + AUCTION_EVENT_RATIO - 1) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO);
            } else {
                long a = std::max(0L, epoch * AUCTION_EVENT_RATIO + AUCTION_EVENT_RATIO - 1 - 20000);
                long b = epoch * AUCTION_EVENT_RATIO + AUCTION_EVENT_RATIO - 1;
                auction = a + rand() % (b - a + 1 + 100);
            }

            if (rand() % 100 > 85) {
                long personId = epoch * PERSON_EVENT_RATIO + PERSON_EVENT_RATIO - 1;
                bidder = (personId / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
            } else {
                long personId = epoch * PERSON_EVENT_RATIO + PERSON_EVENT_RATIO - 1;
                long activePersons = std::min(personId, 60000L);
                long n = rand() % (activePersons + 100);
                bidder = personId + activePersons - n;
            }

            tb.append(std::make_tuple((int64_t) auction, (int64_t) bidder, /*writes a decimal for 0.1*/ (int64_t) 10));
        }

        table = tb.finishTable();
    }
};

class NX1 : public AbstractNexmarkBenchmark {
  public:
    NX1(BenchmarkOptions options) : AbstractNexmarkBenchmark(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> piepeline;
    void setup() override {
        AbstractNexmarkBenchmark::setup();
        plan = NES::Runtime::Execution::Nexmark1::getPipelinePlan(table, bm);
    }
    void compileQuery(Timer<>& compileTimeTimer) override {

        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;

        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        piepeline = provider->create(pipeline1.pipeline, options);
        // we call setup here to force compilation
        piepeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("compilation");
        compileTimeTimer.pause();
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);
        executionTimeTimer.start();
        piepeline->setup(*pipeline1.ctx);
        for (auto& chunk : table->getChunks()) {
            piepeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        piepeline->stop(*pipeline1.ctx);
    }
};

class NX2 : public AbstractNexmarkBenchmark {
  public:
    NX2(BenchmarkOptions options) : AbstractNexmarkBenchmark(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    void setup() override {
        AbstractNexmarkBenchmark::setup();
        plan = NES::Runtime::Execution::Nexmark2::getPipelinePlan(table, bm);
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

class YSBRunner : public BenchmarkRunner {
  public:
    YSBRunner(BenchmarkOptions options) : BenchmarkRunner(options){};
    PipelinePlan plan;
    std::unique_ptr<ExecutablePipelineStage> aggExecutablePipeline;
    std::unique_ptr<ExecutablePipelineStage> emitExecutablePipeline;
    std::unique_ptr<Table> table;
    void setup() override {
        auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                          ->addField("user_id", BasicType::UINT64)
                          ->addField("page_id", BasicType::UINT64)
                          ->addField("campaign_id", BasicType::UINT64)
                          ->addField("ad_type", BasicType::UINT64)
                          ->addField("event_type", BasicType::UINT64)
                          ->addField("current_ms", BasicType::UINT64)
                          ->addField("ip", BasicType::UINT64)
                          ->addField("d1", BasicType::UINT64)
                          ->addField("d2", BasicType::UINT64)
                          ->addField("d3", BasicType::UINT32)
                          ->addField("d4", BasicType::UINT16);
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, table_bm->getBufferSize());
        auto tb = TableBuilder(table_bm, memoryLayout);

        // create 100MB of data
        int32_t records = (1024 * 1024 * 5) / sizeof(int32_t);
        for (int32_t i = 0; i < records; i++) {

            auto campaign_id = rand() % 1000;
            auto event_type = i % 3; /*
            dynamicBuffer[currentRecord]["user_id"].write<uint64_t>(1);
            dynamicBuffer[currentRecord]["page_id"].write<uint64_t>(0);
            dynamicBuffer[currentRecord]["campaign_id"].write<uint64_t>(campaign_id);
            dynamicBuffer[currentRecord]["ad_type"].write<uint64_t>(0);
            dynamicBuffer[currentRecord]["event_type"].write<uint64_t>(event_type);
            dynamicBuffer[currentRecord]["current_ms"].write<uint64_t>(ts);
            dynamicBuffer[currentRecord]["ip"].write<uint64_t>(0x01020304);
            dynamicBuffer[currentRecord]["d1"].write<uint64_t>(1);
            dynamicBuffer[currentRecord]["d2"].write<uint64_t>(1);
            dynamicBuffer[currentRecord]["d3"].write<uint32_t>(1);
            dynamicBuffer[currentRecord]["d4"].write<uint16_t>(1);*/
            tb.append(std::make_tuple((uint64_t) 1,
                                      (uint64_t) 0,
                                      (uint64_t) campaign_id,
                                      (uint64_t) 0,
                                      (uint64_t) event_type,
                                      (uint64_t) 1,
                                      (uint64_t) 0x01020304,
                                      (uint64_t) 1,
                                      (uint64_t) 1,
                                      (uint32_t) 1,
                                      (uint16_t) 1));
        }
        table = tb.finishTable();
        plan = NES::Runtime::Execution::YSB::getPipelinePlan(table, bm);
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
        aggExecutablePipeline->stop(*pipeline1.ctx);
    }
    void runQuery(Timer<>& executionTimeTimer) override {
        auto pipeline1 = plan.getPipeline(0);

        aggExecutablePipeline->setup(*pipeline1.ctx);
        executionTimeTimer.start();
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
    if (query == "NX1") {
        NES::Runtime::Execution::NX1(options).run();
    } else if (query == "NX2") {
        NES::Runtime::Execution::NX2(options).run();
    } else if (query == "YSB") {
        NES::Runtime::Execution::YSBRunner(options).run();
    }
    return 0;
}
