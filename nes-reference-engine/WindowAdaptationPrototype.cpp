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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Configuration/WorkerConfiguration.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <CompiledQueryPlan.hpp>
#include <CompositeStatisticListener.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <ModelCatalog.hpp>
#include <Module.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <PipelineState.hpp>
#include <PipelineStateBufferRef.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <ReferenceNodeEngine.hpp>
#include <function.hpp>
#include <options.hpp>

namespace
{
using namespace NES;

void registerCatalog(
    const std::shared_ptr<SourceCatalog>& sources, const std::shared_ptr<SinkCatalog>& sinks, const std::filesystem::path& output)
{
    StatementBinder binder(sources, [](auto* query) { return AntlrSQLQueryParser::bindLogicalQueryPlan(query); });
    SourceStatementHandler sourceHandler(sources, DefaultHost{"localhost"});
    SinkStatementHandler sinkHandler(sinks, DefaultHost{"localhost"});
    const std::vector<std::string> declarations{
        "CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL, ts UINT64 NOT NULL);",
        R"(CREATE PHYSICAL SOURCE FOR stream TYPE Generator SET(
            'CSV' AS INPUT_FORMATTER."TYPE",
            'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
            'FIXED' AS "SOURCE".GENERATOR_RATE_TYPE,
            'emit_rate 100000' AS "SOURCE".GENERATOR_RATE_CONFIG,
            'SEQUENCE UINT64 0 1000000000 1, SEQUENCE UINT64 0 1000000000 1' AS "SOURCE".GENERATOR_SCHEMA);)",
        "CREATE SINK output(start UINT64 NOT NULL, end UINT64 NOT NULL, parity UINT64 NOT NULL, rowCount UINT64 NOT NULL, total UINT64 NOT "
        "NULL) TYPE File SET('"
            + output.string() + R"(' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT, 'true' AS "SINK".APPEND);)"};

    for (const auto& sql : declarations)
    {
        auto statement = binder.parseAndBindSingle(sql);
        if (not statement)
        {
            throw std::runtime_error(statement.error().what());
        }
        if (const auto* logical = std::get_if<CreateLogicalSourceStatement>(&*statement))
        {
            if (auto result = sourceHandler(*logical); not result)
            {
                throw std::runtime_error(result.error().what());
            }
        }
        else if (const auto* physical = std::get_if<CreatePhysicalSourceStatement>(&*statement))
        {
            if (auto result = sourceHandler(*physical); not result)
            {
                throw std::runtime_error(result.error().what());
            }
        }
        else if (const auto* sink = std::get_if<CreateSinkStatement>(&*statement))
        {
            if (auto result = sinkHandler(*sink); not result)
            {
                throw std::runtime_error(result.error().what());
            }
        }
        else
        {
            throw std::runtime_error("Unexpected catalog statement");
        }
    }
}

std::unique_ptr<CompiledQueryPlan> compile(
    const std::string& sql,
    QueryId queryId,
    const RuleBasedOptimizer& optimizer,
    QueryCompilation::QueryCompiler& compiler,
    std::optional<LogicalPlan>& donorPlan)
{
    auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
    plan.setQueryId(queryId);
    auto optimized = optimizer.optimize(std::move(plan));
    auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(optimized);
    request->donorQueryPlan = donorPlan;
    if (std::getenv("NES_WINDOW_ADAPTATION_DUMP_IR") != nullptr)
    {
        request->dumpCompilationResult = DumpMode{DumpMode::Options::FILE, false};
    }
    auto result = compiler.compileQuery(std::move(request));
    donorPlan = std::move(optimized);
    return result;
}

size_t waitForOutput(const std::filesystem::path& output, size_t previousBytes)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline)
    {
        std::error_code error;
        const auto size = std::filesystem::file_size(output, error);
        if (not error && size > previousBytes)
        {
            return size;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw std::runtime_error("Timed out waiting for window output");
}

bool containsAggregationBuild(const CompiledQueryPlan& plan, PipelineId pipelineId)
{
    for (const auto& pipeline : plan.pipelines)
    {
        if (pipeline->id != pipelineId)
        {
            continue;
        }
        const auto& stage = dynamic_cast<const CompiledExecutablePipelineStage&>(*pipeline->stage);
        auto op = stage.getPipeline().getRootOperator();
        while (true)
        {
            if (op.tryGet<AggregationBuildPhysicalOperator>())
            {
                return true;
            }
            const auto child = op.getChild();
            if (not child)
            {
                return false;
            }
            op = *child;
        }
    }
    throw std::runtime_error("Pipeline is missing from compiled plan");
}

struct PlanShape
{
    OriginId source;
    PipelineId input;
    PipelineId sink;
    std::vector<PipelineId> pipelines;
};

PlanShape inspect(const CompiledQueryPlan& plan)
{
    if (plan.sources.size() != 1 || plan.sinks.size() != 1 || plan.sources.front().successors.size() != 1)
    {
        throw std::runtime_error("Expected one source, one sink, and one input pipeline");
    }
    const auto input = plan.sources.front().successors.front().lock();
    if (not input || containsAggregationBuild(plan, input->id))
    {
        throw std::runtime_error("Window aggregation was fused into the input formatter pipeline");
    }
    PlanShape shape{.source = plan.sources.front().originId, .input = input->id, .sink = plan.sinks.front().id, .pipelines = {}};
    for (const auto& pipeline : plan.pipelines)
    {
        shape.pipelines.push_back(pipeline->id);
    }
    size_t buildCount = 0;
    for (const auto& pipeline : plan.pipelines)
    {
        buildCount += containsAggregationBuild(plan, pipeline->id);
    }
    if (buildCount != 1)
    {
        throw std::runtime_error("Expected exactly one window aggregation build pipeline");
    }
    return shape;
}

ExecutableQueryPlan::SharingIds sharingFor(const PlanShape& shape)
{
    ExecutableQueryPlan::SharingIds sharing;
    sharing.sources.emplace(shape.source, "prototype:window:source");
    return sharing;
}

std::vector<std::pair<PipelineId, PipelineId>> makeTransfers(const PlanShape& donor, const PlanShape& target)
{
    if (donor.pipelines.size() != target.pipelines.size())
    {
        throw std::runtime_error("Window replacement changed the pipeline topology");
    }
    std::vector<std::pair<PipelineId, PipelineId>> transfers;
    for (size_t index = 0; index < donor.pipelines.size(); ++index)
    {
        const auto from = donor.pipelines[index];
        const auto to = target.pipelines[index];
        if ((from == donor.input) != (to == target.input))
        {
            throw std::runtime_error("Input pipeline moved in the replacement plan");
        }
        transfers.emplace_back(from, to);
    }
    return transfers;
}

struct TransferCounter final : QueryEngineStatisticListener
{
    void onEvent(Event event) override
    {
        if (std::holds_alternative<PipelineStateExport>(event))
        {
            exports.fetch_add(1);
        }
        else if (std::holds_alternative<PipelineStateImport>(event))
        {
            imports.fetch_add(1);
        }
    }

    std::atomic_size_t exports = 0;
    std::atomic_size_t imports = 0;
};

void rejectMissingDonorAbsorb()
{
    throw NotImplemented("Missing donor definition");
}

struct MissingDonorOperator final : PhysicalOperatorConcept
{
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return std::nullopt; }

    void setChild(PhysicalOperator) override { throw std::runtime_error("Unexpected child"); }

    void setup(ExecutionContext&, CompilationContext&) const override { }

    void open(ExecutionContext&, RecordBuffer&) const override { }

    void close(ExecutionContext&, RecordBuffer&) const override { }

    void lowerAbsorb(nautilus::val<PipelineStateReader*>, nautilus::val<PipelineExecutionContext*>) const override
    {
        nautilus::invoke(rejectMissingDonorAbsorb);
    }
};

struct GuardContext final : PipelineExecutionContext
{
    std::shared_ptr<BufferManager> buffers
        = BufferManager::create(1024 * 1024, 0.5, BufferAlignment(64), 1024, std::make_shared<NesDefaultMemoryAllocator>());
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { throw std::runtime_error("Unexpected repeat"); }

    TupleBuffer allocateTupleBuffer() override { return buffers->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return buffers; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return handlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& replacement) override
    {
        handlers = replacement;
    }
};

void verifyMissingDonorThrows()
{
    auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(MissingDonorOperator{}));
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    CompiledExecutablePipelineStage stage(pipeline, {}, options);
    GuardContext context;
    stage.start(context);
    try
    {
        PipelineStateBuilder state;
        stage.absorb(state.finish(context.getBufferManager()), context);
    }
    catch (const Exception& error)
    {
        if (error.code() == ErrorCode::NotImplemented)
        {
            return;
        }
        throw;
    }
    throw std::runtime_error("Compiled absorb accepted state without a donor definition");
}

void verifyCompiledStateBufferRef()
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    nautilus::engine::NautilusEngine engine(options);
    auto module = engine.createModule();
    const std::function<void(nautilus::val<const TupleBuffer*>)> emit = [](nautilus::val<const TupleBuffer*> buffer)
    {
        PipelineStateBufferRef state(BorrowedNautilusBuffer::from(buffer));
        state.writeU64(42);
        for (nautilus::val<uint64_t> index = 0; index < 3; ++index)
        {
            state.writeU64(index + 1);
        }
    };
    const std::function<void(nautilus::val<const TupleBuffer*>, nautilus::val<uint64_t*>)> absorb
        = [](nautilus::val<const TupleBuffer*> buffer, nautilus::val<uint64_t*> result)
    {
        PipelineStateBufferRef state(BorrowedNautilusBuffer::from(buffer));
        nautilus::val<uint64_t> sum = state.readU64();
        for (nautilus::val<uint64_t> index = 0; index < 3; ++index)
        {
            sum = sum + state.readU64();
        }
        *result = sum;
    };
    module.registerFunction("emit_state_ref", emit);
    module.registerFunction("absorb_state_ref", absorb);
    auto compiled = module.compile();
    auto compiledEmit = compiled.getFunction<void(const TupleBuffer*)>("emit_state_ref");
    auto compiledAbsorb = compiled.getFunction<void(const TupleBuffer*, uint64_t*)>("absorb_state_ref");
    GuardContext context;
    auto buffer = context.buffers->getUnpooledBuffer(4 * sizeof(uint64_t));
    if (not buffer)
    {
        throw std::runtime_error("Could not allocate compiled state buffer");
    }
    compiledEmit(std::addressof(*buffer));
    uint64_t sum = 0;
    compiledAbsorb(std::addressof(*buffer), std::addressof(sum));
    if (sum != 48)
    {
        throw std::runtime_error("Compiled state buffer ref changed the payload");
    }
}
}

int main(int argc, char** argv)
{
    try
    {
        NES::Logger::setupLogging("nes-reference-window-adaptation-prototype.log", NES::LogLevel::LOG_ERROR);
        NES::loadBuiltinPlugins();
        verifyMissingDonorThrows();
        verifyCompiledStateBufferRef();
        if (argc < 2 || argc > 3)
        {
            std::cerr << "Usage: nes-reference-window-adaptation-prototype OUTPUT.csv [DURATION_SECONDS]\n";
            return 2;
        }
        const std::filesystem::path output(argv[1]);
        std::filesystem::remove(output);
        const auto durationSeconds = argc == 3 ? std::stoul(argv[2]) : 10;
        if (durationSeconds == 0)
        {
            throw std::runtime_error("Duration must be positive");
        }
        auto sources = std::make_shared<NES::SourceCatalog>();
        auto sinks = std::make_shared<NES::SinkCatalog>();
        auto models = std::make_shared<NES::ModelCatalog>();
        registerCatalog(sources, sinks, output);

        const auto queryId = NES::QueryId::createLocal(NES::LocalQueryId(NES::generateUUID()));
        NES::RuleBasedOptimizer optimizer(NES::QueryOptimizerConfiguration{}, sources, sinks, models);
        NES::WorkerConfiguration configuration;
        NES::QueryCompilation::QueryCompiler compiler(configuration.defaultQueryExecution);
        const std::string sql = "SELECT start, end, parity, COUNT(*) AS rowCount, SUM(id) AS total FROM "
                                "(SELECT id, id % 2 AS parity, shifted + 1 AS event_time FROM "
                                "(SELECT id, ts + 1 AS shifted FROM stream)) GROUP BY (parity) "
                                "WINDOW TUMBLING(event_time, size 100 ms) INTO output;";
        std::optional<NES::LogicalPlan> donorPlan;
        auto original = compile(sql, queryId, optimizer, compiler, donorPlan);
        auto current = inspect(*original);
        uint64_t adaptations = 0;

        {
            auto statistics = std::make_shared<NES::CompositeStatisticListener>();
            auto transfersObserved = std::make_shared<TransferCounter>();
            statistics->addQueryEngineListener(transfersObserved);
            NES::ReferenceNodeEngine engine(configuration, statistics, NES::Host{"localhost"});
            engine.startQuery(queryId, std::move(original), sharingFor(current));
            auto previousBytes = waitForOutput(output, 64);
            const auto started = std::chrono::steady_clock::now();
            const auto deadline = started + std::chrono::seconds(durationSeconds);
            while (std::chrono::steady_clock::now() < deadline || adaptations == 0)
            {
                auto replacement = compile(sql, queryId, optimizer, compiler, donorPlan);
                auto target = inspect(*replacement);
                auto transfers = makeTransfers(current, target);
                if (not engine.adaptQuery(std::move(replacement), transfers, sharingFor(target)))
                {
                    throw std::runtime_error("Reference query manager rejected adaptation");
                }
                ++adaptations;
                const auto exports = transfersObserved->exports.load();
                const auto imports = transfersObserved->imports.load();
                if (exports != adaptations * transfers.size() || imports != exports)
                {
                    throw std::runtime_error("Pipeline state was not transferred through emit and absorb");
                }
                current = std::move(target);
                previousBytes = waitForOutput(output, previousBytes);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            engine.stopQuery(queryId);
        }

        std::ifstream result(output);
        std::string line;
        std::getline(result, line);
        uint64_t previousEnd = 0;
        size_t windows = 0;
        size_t rowsInWindow = 0;
        bool sawEven = false;
        bool sawOdd = false;
        while (std::getline(result, line))
        {
            const auto firstComma = line.find(',');
            const auto secondComma = line.find(',', firstComma + 1);
            const auto thirdComma = line.find(',', secondComma + 1);
            const auto fourthComma = line.find(',', thirdComma + 1);
            if (firstComma == std::string::npos || secondComma == std::string::npos || thirdComma == std::string::npos
                || fourthComma == std::string::npos)
            {
                throw std::runtime_error("Malformed window output");
            }
            const auto start = std::stoull(line.substr(0, firstComma));
            const auto end = std::stoull(line.substr(firstComma + 1, secondComma - firstComma - 1));
            const auto parity = std::stoull(line.substr(secondComma + 1, thirdComma - secondComma - 1));
            const auto count = std::stoull(line.substr(thirdComma + 1, fourthComma - thirdComma - 1));
            const auto total = std::stoull(line.substr(fourthComma + 1));
            if (rowsInWindow == 0)
            {
                if (windows != 0 && start != previousEnd)
                {
                    throw std::runtime_error("Window sequence changed across adaptation");
                }
                previousEnd = end;
                sawEven = false;
                sawOdd = false;
            }
            else if (end != previousEnd)
            {
                throw std::runtime_error("Window key result is missing");
            }
            const uint64_t expectedCount = windows == 0 ? 49 : 50;
            const auto expectedTotal = windows == 0 ? (parity == 0 ? 2352 : 2401) : 50 * start + (parity == 0 ? 2350 : 2400);
            if (end != start + 100 || parity > 1 || count != expectedCount || total != expectedTotal || (parity == 0 ? sawEven : sawOdd))
            {
                throw std::runtime_error("Window aggregation state changed across adaptation");
            }
            (parity == 0 ? sawEven : sawOdd) = true;
            if (++rowsInWindow == 2)
            {
                if (not sawEven || not sawOdd)
                {
                    throw std::runtime_error("Both window keys must be present");
                }
                rowsInWindow = 0;
                ++windows;
            }
        }
        if (rowsInWindow != 0 || windows < adaptations + 1)
        {
            throw std::runtime_error("Too few completed windows to verify adaptation");
        }
        std::cout << "Migrated keyed window aggregation " << adaptations << " times; verified " << windows << " ordered windows\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Window adaptation prototype failed: " << error.what() << '\n';
        return 1;
    }
}
