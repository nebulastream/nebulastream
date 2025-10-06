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

#include <SingleNodeWorker.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <utility>
#include <unistd.h>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Common.hpp>
#include <Util/DumpMode.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticPrinter.hpp>
#include <thread>
#include <cstring>
#include <unordered_map>
#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/SerializableHJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/SerializableNLJOperatorHandler.hpp>
#include <Nautilus/Recovery/CheckpointCoordinator.hpp>
#include <Nautilus/Recovery/CheckpointManager.hpp>
#include <Nautilus/Recovery/RecoveryManager.hpp>
#include <Nautilus/State/Reflection/PipelineState.hpp>
#include <RecoveredOperatorHandlersRegistry.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration)
    : listener(std::make_shared<PrintingStatisticListener>(
          fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid())))
    , nodeEngine(NodeEngineBuilder(
                      configuration.workerConfiguration,
                      NES::Util::copyPtr(listener),
                      NES::Util::copyPtr(listener))
                      .build())
    , optimizer(std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryExecution))
    , compiler(std::make_unique<QueryCompilation::QueryCompiler>())
    , configuration(configuration)
{
    if (configuration.workerConfiguration.bufferSizeInBytes.getValue()
        < configuration.workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue())
    {
        throw InvalidConfigParameter(
            "Currently, we require the bufferSizeInBytes {} to be at least the operatorBufferSize {}",
            configuration.workerConfiguration.bufferSizeInBytes.getValue(),
            configuration.workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue());
    }
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        plan.setQueryId(QueryId(queryIdCounter++));
        // Keep a copy of the original plan to support in-process recovery testing
        originalPlans[plan.getQueryId()] = plan;
        // If there are pending recovered handlers, bind them to this new query id
        RecoveredOperatorHandlersRegistry::consumePendingForQuery(plan.getQueryId());
        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        request->dumpCompilationResult = configuration.workerConfiguration.dumpQueryCompilationIntermediateRepresentations.getValue();
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successfull query compilation or exception, but got nothing");
        return nodeEngine->registerCompiledQueryPlan(std::move(result));
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::startQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->startQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId, type);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::unregisterQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->unregisterQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<QuerySummary, Exception> SingleNodeWorker::getQuerySummary(QueryId queryId) const noexcept
{
    CPPTRACE_TRY
    {
        auto summary = nodeEngine->getQueryLog()->getQuerySummary(queryId);
        if (not summary.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return summary.value();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

std::expected<void, Exception> SingleNodeWorker::exportCheckpoint(QueryId queryId, const std::string& path) noexcept
{
    try {
        // Build a PipelineState and attempt to capture serializable aggregation handlers
        NES::State::PipelineState ps{};
        ps.version = 1;
        ps.queryId = queryId.getRawValue();
        ps.pipelineId = 0;
        ps.createdTimestampNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::system_clock::now().time_since_epoch()).count());
        ps.progress.version = 1;
        ps.progress.lastWatermark = 0;

        // Try to snapshot operator handlers from running query
        auto snapshots = nodeEngine->snapshotOperatorHandlers(queryId);
        // Queries in systest can be very short-lived; wait briefly for pipelines to start and register handlers
        if (snapshots.empty()) {
            for (int i = 0; i < 40 && snapshots.empty(); ++i) { // up to ~200ms
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                snapshots = nodeEngine->snapshotOperatorHandlers(queryId);
            }
        }
        NES_INFO("exportCheckpoint: captured {} operator handler snapshots for query {}", snapshots.size(), queryId);
        uint64_t maxAggWatermark = 0;
        std::unordered_map<uint64_t, uint64_t> originToWatermark;
        for (const auto& snap : snapshots) {
            auto blob = NES::State::OperatorStateBlob{};
            blob.header.operatorId = snap.id.getRawValue();
            blob.header.version = 1;

            // Aggregation handlers (real state)
            if (auto* agg = dynamic_cast<SerializableAggregationOperatorHandler*>(snap.handler.get())) {
                // Best-effort: wait for some observable state to reduce timing races
                agg->waitForDataOrTimeout(200, 5);
                // Capture live slice/window state prior to serialization
                agg->captureState();
                auto tb = agg->serialize(nodeEngine->getBufferManager().get());
                blob.bytes.resize(tb.getBufferSize());
                if (tb.getBufferSize() > 0) {
                    std::memcpy(blob.bytes.data(), tb.getBuffer(), tb.getBufferSize());
                }
                blob.header.kind = NES::State::OperatorStateTag::Kind::Aggregation;
                ps.operators.emplace_back(std::move(blob));
                // Track watermark for pipeline progress
                maxAggWatermark = std::max<uint64_t>(maxAggWatermark, agg->getState().metadata.lastWatermark);
                // Populate per-origin progress lastWatermark (best effort: same baseline for each input origin)
                for (auto oid : agg->getInputOrigins()) {
                    auto id = oid.getRawValue();
                    auto& ow = originToWatermark[id];
                    ow = std::max<uint64_t>(ow, agg->getState().metadata.lastWatermark);
                }
                continue;
            }
            // Serializable Hash Join handlers (real state)
            if (auto* hj = dynamic_cast<SerializableHJOperatorHandler*>(snap.handler.get())) {
                hj->waitForDataOrTimeout(200, 5);
                hj->captureState();
                auto tb = hj->serialize(nodeEngine->getBufferManager().get());
                blob.bytes.resize(tb.getBufferSize());
                if (tb.getBufferSize() > 0) {
                    std::memcpy(blob.bytes.data(), tb.getBuffer(), tb.getBufferSize());
                }
                blob.header.kind = NES::State::OperatorStateTag::Kind::HashJoin;
                ps.operators.emplace_back(std::move(blob));
                // Track watermark
                maxAggWatermark = std::max<uint64_t>(maxAggWatermark, hj->getState().metadata.lastWatermark);
                // Per-origin lastWatermark best-effort
                for (auto oid : hj->getInputOrigins()) {
                    auto id = oid.getRawValue();
                    auto& ow = originToWatermark[id];
                    ow = std::max<uint64_t>(ow, hj->getState().metadata.lastWatermark);
                }
                continue;
            }
            // Serializable NLJ handlers (real state)
            if (auto* nlj = dynamic_cast<SerializableNLJOperatorHandler*>(snap.handler.get())) {
                nlj->waitForDataOrTimeout(200, 5);
                nlj->captureState();
                auto tb = nlj->serialize(nodeEngine->getBufferManager().get());
                blob.bytes.resize(tb.getBufferSize());
                if (tb.getBufferSize() > 0) {
                    std::memcpy(blob.bytes.data(), tb.getBuffer(), tb.getBufferSize());
                }
                blob.header.kind = NES::State::OperatorStateTag::Kind::NestedLoopJoin;
                ps.operators.emplace_back(std::move(blob));
                // Track watermark
                maxAggWatermark = std::max<uint64_t>(maxAggWatermark, nlj->getState().metadata.lastWatermark);
                for (auto oid : nlj->getInputOrigins()) {
                    auto id = oid.getRawValue();
                    auto& ow = originToWatermark[id];
                    ow = std::max<uint64_t>(ow, nlj->getState().metadata.lastWatermark);
                }
                continue;
            }
            // Tag Join handlers with empty blobs to enable RecoveryManager factory reconstruction later
            if (dynamic_cast<NLJOperatorHandler*>(snap.handler.get())) {
                blob.header.kind = NES::State::OperatorStateTag::Kind::NestedLoopJoin;
                ps.operators.emplace_back(std::move(blob));
                continue;
            }
            if (dynamic_cast<HJOperatorHandler*>(snap.handler.get())) {
                blob.header.kind = NES::State::OperatorStateTag::Kind::HashJoin;
                ps.operators.emplace_back(std::move(blob));
                continue;
            }
            // Other handlers ignored for now
        }

        // Set pipeline progress watermark based on captured aggregation handlers (best effort)
        if (maxAggWatermark > 0) {
            ps.progress.lastWatermark = maxAggWatermark;
        }
        // Fill per-origin entries
        for (const auto& kv : originToWatermark) {
            NES::State::ProgressMetadata::OriginProgress op{};
            op.originId = kv.first;
            op.processedRecords = 0; // unknown per-origin; optional extension later
            op.lastWatermark = kv.second;
            ps.progress.origins.emplace_back(op);
        }

        // If serializable aggregation is enabled, fail-fast when nothing was captured
        if (configuration.workerConfiguration.defaultQueryExecution.enableSerializableAggregation.getValue() && ps.operators.empty()) {
            return std::unexpected(InvalidConfigParameter(
                "Checkpoint capture found no operator state while serializable aggregation is enabled. Try increasing checkpointAfterMs."));
        }

        // Log captured operators/kinds for visibility
        for (const auto& op : ps.operators) {
            NES_INFO("Checkpoint includes operatorId={} kind={} bytes={}",
                     op.header.operatorId,
                     static_cast<int>(op.header.kind),
                     op.bytes.size());
        }

        NES::Recovery::CheckpointCoordinator::writeCheckpoint(ps, path);

        auto it = originalPlans.find(queryId);
        if (it != originalPlans.end()) {
            checkpointPlanByPath[path] = it->second;
        }
        return {};
    } catch (...) {
        return std::unexpected{wrapExternalException()};
    }
}

std::expected<QueryId, Exception> SingleNodeWorker::importCheckpoint(const std::string& path) noexcept
{
    try {
        // Load checkpoint
        NES::Recovery::CheckpointManager cm;
        auto ps = cm.recover(path);

        // Build RecoveryManager args with default factories for joins
        NES::Recovery::RecoveryManager::RecreateArgs rargs{};
        rargs.bufferProvider = nodeEngine->getBufferManager().get();
        for (const auto& blob : ps.operators) {
            // Provide generic construction args (origins and slice store); values are placeholders for validation only
            NES::OriginId out{0};
            rargs.operatorArgsById.emplace(blob.header.operatorId, NES::Recovery::OperatorConstructionArgs{
                .inputOrigins = std::vector<NES::OriginId>{},
                .outputOriginId = out,
                .makeSliceStore = [](){ return std::make_unique<NES::DefaultTimeBasedSliceStore>(1000, 1000); },
            });

            // Factories for joins
            if (blob.header.kind == NES::State::OperatorStateTag::Kind::NestedLoopJoin) {
                rargs.operatorFactoryById.emplace(blob.header.operatorId, [=](const NES::State::OperatorStateBlob&){
                    return std::make_shared<NES::NLJOperatorHandler>(
                        std::vector<NES::OriginId>{}, NES::OriginId(0), std::make_unique<NES::DefaultTimeBasedSliceStore>(1000, 1000));
                });
            } else if (blob.header.kind == NES::State::OperatorStateTag::Kind::HashJoin) {
                rargs.operatorFactoryById.emplace(blob.header.operatorId, [=](const NES::State::OperatorStateBlob&){
                    return std::make_shared<NES::HJOperatorHandler>(
                        std::vector<NES::OriginId>{}, NES::OriginId(0), std::make_unique<NES::DefaultTimeBasedSliceStore>(1000, 1000));
                });
            }
        }

        // Attempt recovery (validates serialization path and factories); handlers are not yet injected into pipelines
        auto recovered = NES::Recovery::RecoveryManager::recoverPipeline(path, rargs);
        NES::RecoveredOperatorHandlersRegistry::HandlerMap recoveredMap;
        for (size_t i = 0; i < recovered.pipelineState.operators.size() && i < recovered.handlers.size(); ++i) {
            auto opId = recovered.pipelineState.operators[i].header.operatorId;
            recoveredMap.emplace(OperatorHandlerId(opId), recovered.handlers[i]);
        }
        RecoveredOperatorHandlersRegistry::setPending(std::move(recoveredMap));

        // Re-register original logical plan as executable query plan
        auto it = checkpointPlanByPath.find(path);
        if (it == checkpointPlanByPath.end()) {
            return std::unexpected(InvalidConfigParameter("No stored plan for checkpoint {}", path));
        }
        return registerQuery(it->second);
    } catch (...) {
        return std::unexpected{wrapExternalException()};
    }
}

}
