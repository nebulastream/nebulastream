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
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <CompiledQueryPlan.hpp>
#include <CompositeStatisticListener.hpp>
#include <ExecutableQueryPlan.hpp>
#include <ModelCatalog.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <ReferenceNodeEngine.hpp>
#include <SelectionPhysicalOperator.hpp>
#include <VoidSink.hpp>

namespace
{
using namespace NES;

void registerCatalog(const std::shared_ptr<SourceCatalog>& sources, const std::shared_ptr<SinkCatalog>& sinks)
{
    StatementBinder binder(sources, [](auto* query) { return AntlrSQLQueryParser::bindLogicalQueryPlan(query); });
    SourceStatementHandler sourceHandler(sources, DefaultHost{"localhost"});
    SinkStatementHandler sinkHandler(sinks, DefaultHost{"localhost"});
    const std::vector<std::string> declarations{
        "CREATE LOGICAL SOURCE stream(id UINT64 NOT NULL, value UINT64 NOT NULL);",
        R"(CREATE PHYSICAL SOURCE FOR stream TYPE Generator SET(
            'CSV' AS INPUT_FORMATTER."TYPE",
            'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
            'FIXED' AS "SOURCE".GENERATOR_RATE_TYPE,
            'emit_rate 100000' AS "SOURCE".GENERATOR_RATE_CONFIG,
            'SEQUENCE UINT64 0 1000000000 1, SEQUENCE UINT64 0 100 1' AS "SOURCE".GENERATOR_SCHEMA);)",
        "CREATE SINK output(id UINT64 NOT NULL) TYPE Void;"};

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

std::unique_ptr<CompiledQueryPlan>
compile(const std::string& sql, QueryId queryId, const RuleBasedOptimizer& optimizer, QueryCompilation::QueryCompiler& compiler)
{
    auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
    plan.setQueryId(queryId);
    auto optimized = optimizer.optimize(std::move(plan));
    return compiler.compileQuery(std::make_unique<QueryCompilation::QueryCompilationRequest>(std::move(optimized)));
}

void waitForTuples(uint64_t previousCount)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (VoidSink::getMetrics().receivedTuples > previousCount)
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw std::runtime_error("Timed out waiting for Void sink tuples");
}

bool containsSelection(const CompiledQueryPlan& plan, PipelineId pipelineId)
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
            if (op.tryGet<SelectionPhysicalOperator>())
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
    throw std::runtime_error("Input pipeline is missing from compiled plan");
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
    if (not input || containsSelection(plan, input->id))
    {
        throw std::runtime_error("Predicate was fused into the input formatter pipeline");
    }
    PlanShape shape{.source = plan.sources.front().originId, .input = input->id, .sink = plan.sinks.front().id, .pipelines = {}};
    for (const auto& pipeline : plan.pipelines)
    {
        shape.pipelines.push_back(pipeline->id);
    }
    return shape;
}

ExecutableQueryPlan::SharingIds sharingFor(const PlanShape& shape)
{
    ExecutableQueryPlan::SharingIds sharing;
    sharing.sources.emplace(shape.source, "prototype:source");
    sharing.pipelines.emplace(shape.input, "prototype:input");
    return sharing;
}

std::vector<std::pair<PipelineId, PipelineId>> makeTransfers(const PlanShape& donor, const PlanShape& target)
{
    if (donor.pipelines.size() != target.pipelines.size())
    {
        throw std::runtime_error("Predicate reorder changed the pipeline topology");
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
        if (from != donor.input)
        {
            transfers.emplace_back(from, to);
        }
    }
    transfers.emplace_back(donor.sink, target.sink);
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
}

int main(int argc, char** argv)
{
    try
    {
        NES::Logger::setupLogging("nes-reference-adaptation-prototype.log", NES::LogLevel::LOG_ERROR);
        NES::loadBuiltinPlugins();
        if (argc > 2)
        {
            std::cerr << "Usage: nes-reference-adaptation-prototype [DURATION_SECONDS]\n";
            return 2;
        }
        const auto durationSeconds = argc == 2 ? std::stoul(argv[1]) : 10;
        if (durationSeconds == 0)
        {
            throw std::runtime_error("Duration must be positive");
        }
        auto sources = std::make_shared<NES::SourceCatalog>();
        auto sinks = std::make_shared<NES::SinkCatalog>();
        auto models = std::make_shared<NES::ModelCatalog>();
        registerCatalog(sources, sinks);

        const auto queryId = NES::QueryId::createLocal(NES::LocalQueryId(NES::generateUUID()));
        NES::RuleBasedOptimizer optimizer(NES::QueryOptimizerConfiguration{}, sources, sinks, models);
        NES::WorkerConfiguration configuration;
        NES::QueryCompilation::QueryCompiler compiler(configuration.defaultQueryExecution);
        const std::string first = "SELECT id FROM (SELECT id, adjusted_id + 1 AS predicate_id, adjusted + 1 AS predicate_value FROM "
                                  "(SELECT id, id + 1 AS adjusted_id, value + 1 AS adjusted FROM stream)) WHERE predicate_id > 12 "
                                  "AND predicate_value < 1002 INTO output;";
        const std::string second = "SELECT id FROM (SELECT id, adjusted_id + 1 AS predicate_id, adjusted + 1 AS predicate_value FROM "
                                   "(SELECT id, id + 1 AS adjusted_id, value + 1 AS adjusted FROM stream)) WHERE predicate_value < "
                                   "1002 AND predicate_id > 12 INTO output;";
        auto original = compile(first, queryId, optimizer, compiler);
        auto current = inspect(*original);
        uint64_t adaptations = 0;
        const auto initialCount = NES::VoidSink::getMetrics().receivedTuples;

        {
            auto statistics = std::make_shared<NES::CompositeStatisticListener>();
            auto transfersObserved = std::make_shared<TransferCounter>();
            statistics->addQueryEngineListener(transfersObserved);
            NES::ReferenceNodeEngine engine(configuration, statistics, NES::Host{"localhost"});
            engine.startQuery(queryId, std::move(original), sharingFor(current));
            waitForTuples(initialCount);
            const auto started = std::chrono::steady_clock::now();
            const auto deadline = started + std::chrono::seconds(durationSeconds);
            auto lastReport = started;
            auto lastCount = NES::VoidSink::getMetrics().receivedTuples;
            while (std::chrono::steady_clock::now() < deadline || adaptations == 0)
            {
                const auto& nextSql = adaptations % 2 == 0 ? second : first;
                auto replacement = compile(nextSql, queryId, optimizer, compiler);
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
                const auto metrics = NES::VoidSink::getMetrics();
                if (metrics.exports != adaptations || metrics.imports != adaptations
                    || metrics.lastExportedCount != metrics.lastImportedCount)
                {
                    throw std::runtime_error("Void sink tuple count was not migrated");
                }
                current = std::move(target);
                const auto now = std::chrono::steady_clock::now();
                if (now - lastReport >= std::chrono::seconds(1))
                {
                    const auto seconds = std::chrono::duration<double>(now - lastReport).count();
                    std::cout << "Received " << (metrics.receivedTuples - lastCount) / seconds << " tuples/s over " << seconds
                              << " s; adaptations=" << adaptations << "; total=" << metrics.receivedTuples - initialCount << '\n';
                    lastReport = now;
                    lastCount = metrics.receivedTuples;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            engine.stopQuery(queryId);
            const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
            const auto count = NES::VoidSink::getMetrics().receivedTuples - initialCount;
            if (count == 0)
            {
                throw std::runtime_error("Void sink received no tuples");
            }
            std::cout << "Received " << count << " tuples in " << elapsed << " s (" << count / elapsed << " tuples/s) across "
                      << adaptations << " adaptations\n";
        }
        const auto finalMetrics = NES::VoidSink::getMetrics();
        if (finalMetrics.stoppedCount != finalMetrics.receivedTuples)
        {
            throw std::runtime_error("Final Void sink count does not match all received tuples");
        }
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Adaptation prototype failed: " << error.what() << '\n';
        return 1;
    }
}
