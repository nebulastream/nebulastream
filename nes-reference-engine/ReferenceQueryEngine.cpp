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

#include <ReferenceQueryEngine.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

extern "C" {
void* rq_manager_new(void* listener);
void rq_manager_free(void* manager);
void* rq_plan_new(size_t query);
void rq_plan_free(void* plan);
bool rq_plan_add_pipeline(void* plan, size_t id, void* handle, const size_t* successors, size_t count, const char* sharingId);
void rq_plan_add_source(void* plan, size_t id, void* handle, const size_t* successors, size_t count, const char* sharingId);
bool rq_manager_start(void* manager, void* plan);
bool rq_manager_adapt(void* manager, void* plan, const size_t* donors, const size_t* targets, size_t count);
bool rq_manager_stop(void* manager, size_t query);
void rq_source_data(void* sender, void* buffer, uint64_t sequence);
void rq_source_eos(void* sender);
}

namespace NES
{
namespace
{
using Output = void (*)(void*, void*, uint64_t);

class ReferenceExecutionContext final : public PipelineExecutionContext
{
public:
    ReferenceExecutionContext(std::shared_ptr<BufferManager> bufferManager, PipelineId id, Output output, void* outputContext)
        : bufferManager(std::move(bufferManager)), id(id), output(output), outputContext(outputContext)
    {
    }

    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        if (output == nullptr)
        {
            return false;
        }
        output(outputContext, new TupleBuffer(buffer), buffer.getSequenceNumber().getRawValue());
        return true;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override
    {
        throw NotImplemented("repeatTask is unavailable in the reference query engine prototype");
    }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    uint64_t getNumberOfWorkerThreads() const override { return 1; }

    std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    PipelineId getPipelineId() const override { return id; }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        PRECONDITION(handlers != nullptr, "OperatorHandlers were not set");
        return *handlers;
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& replacement) override
    {
        handlers = &replacement;
    }

private:
    std::shared_ptr<BufferManager> bufferManager;
    PipelineId id;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* handlers = nullptr;
    Output output;
    void* outputContext;
};

struct PipelineAdapter
{
    explicit PipelineAdapter(
        std::unique_ptr<ExecutablePipelineStage> stage,
        QueryId queryId,
        PipelineId id,
        std::shared_ptr<BufferManager> buffers,
        std::shared_ptr<QueryEngineStatisticListener> statisticListener)
        : stage(std::move(stage)), queryId(queryId), id(id), buffers(std::move(buffers)), statisticListener(std::move(statisticListener))
    {
    }

    ~PipelineAdapter()
    {
        if (not started)
        {
            return;
        }
        try
        {
            ReferenceExecutionContext context(buffers, id, nullptr, nullptr);
            stage->stop(context);
        }
        catch (const std::exception& error)
        {
            NES_ERROR("Reference engine pipeline stop failed: {}", error.what());
        }
        catch (...)
        {
            NES_ERROR("Reference engine pipeline stop failed with an unknown exception");
        }
    }

    std::mutex mutex;
    std::unique_ptr<ExecutablePipelineStage> stage;
    QueryId queryId;
    PipelineId id;
    std::shared_ptr<BufferManager> buffers;
    std::shared_ptr<QueryEngineStatisticListener> statisticListener;
    bool started = false;
};

struct SourceAdapter
{
    explicit SourceAdapter(std::unique_ptr<SourceHandle> source, std::shared_ptr<BackpressureController> controller)
        : controller(std::move(controller)), source(std::move(source))
    {
    }

    /// Must outlive the source thread's BackpressureListener.
    std::shared_ptr<BackpressureController> controller;
    std::unique_ptr<SourceHandle> source;
};

void* buildRustPlan(
    ExecutableQueryPlan& plan,
    size_t referenceId,
    const std::shared_ptr<BufferManager>& bufferManager,
    const std::shared_ptr<QueryEngineStatisticListener>& statisticListener)
{
    void* rustPlan = rq_plan_new(referenceId);
    try
    {
        for (auto& pipeline : plan.pipelines)
        {
            std::vector<size_t> successors;
            for (const auto& weak : pipeline->successors)
            {
                if (const auto successor = weak.lock())
                {
                    successors.push_back(successor->id.getRawValue());
                }
            }
            auto adapter = std::make_unique<PipelineAdapter>(
                std::move(pipeline->stage), plan.queryId, pipeline->id, bufferManager, statisticListener);
            const auto sharing = plan.sharingIds.pipelines.find(pipeline->id);
            const auto* sharingId = sharing == plan.sharingIds.pipelines.end() ? nullptr : sharing->second.c_str();
            if (not rq_plan_add_pipeline(
                    rustPlan, pipeline->id.getRawValue(), adapter.release(), successors.data(), successors.size(), sharingId))
            {
                throw NotImplemented("reference query engine pipeline initialization failed");
            }
        }
        for (auto& [source, weakSuccessors] : plan.sources)
        {
            std::vector<size_t> successors;
            for (const auto& weak : weakSuccessors)
            {
                if (const auto successor = weak.lock())
                {
                    successors.push_back(successor->id.getRawValue());
                }
            }
            const auto sourceId = source->getSourceId();
            const auto sharing = plan.sharingIds.sources.find(sourceId);
            const auto* sharingId = sharing == plan.sharingIds.sources.end() ? nullptr : sharing->second.c_str();
            const auto controller = plan.sourceBackpressureControllers.find(sourceId);
            auto adapter = std::make_unique<SourceAdapter>(
                std::move(source), controller == plan.sourceBackpressureControllers.end() ? nullptr : std::move(controller->second));
            rq_plan_add_source(rustPlan, sourceId.getRawValue(), adapter.release(), successors.data(), successors.size(), sharingId);
        }
    }
    catch (...)
    {
        rq_plan_free(rustPlan);
        throw;
    }
    return rustPlan;
}
}

extern "C" void nes_ref_release_buffer(void* buffer)
{
    delete static_cast<TupleBuffer*>(buffer);
}

extern "C" bool nes_ref_pipeline_absorb(void* pipeline, void* state)
{
    auto& adapter = *static_cast<PipelineAdapter*>(pipeline);
    try
    {
        const std::scoped_lock lock(adapter.mutex);
        if (not adapter.started)
        {
            ReferenceExecutionContext context(adapter.buffers, adapter.id, nullptr, nullptr);
            adapter.stage->start(context);
            adapter.started = true;
        }
        if (state != nullptr)
        {
            ReferenceExecutionContext context(adapter.buffers, adapter.id, nullptr, nullptr);
            adapter.stage->absorb(*static_cast<TupleBuffer*>(state), context);
            adapter.statisticListener->onEvent(PipelineStateImport(WorkerThreadId(0), adapter.queryId, adapter.id));
        }
        return true;
    }
    catch (const std::exception& error)
    {
        NES_ERROR("Reference engine pipeline initialization failed: {}", error.what());
    }
    catch (...)
    {
        NES_ERROR("Reference engine pipeline initialization failed with an unknown exception");
    }
    return false;
}

extern "C" void nes_ref_pipeline_emit(void* pipeline, Output output, void* outputContext)
{
    auto& adapter = *static_cast<PipelineAdapter*>(pipeline);
    try
    {
        const std::scoped_lock lock(adapter.mutex);
        ReferenceExecutionContext context(adapter.buffers, adapter.id, nullptr, nullptr);
        auto state = adapter.stage->emit(context);
        if (!state)
        {
            return;
        }
        output(outputContext, new TupleBuffer(std::move(state)), 0);
        adapter.statisticListener->onEvent(PipelineStateExport(WorkerThreadId(0), adapter.queryId, adapter.id));
    }
    catch (const std::exception& error)
    {
        NES_ERROR("Reference engine pipeline state export failed: {}", error.what());
    }
    catch (...)
    {
        NES_ERROR("Reference engine pipeline state export failed with an unknown exception");
    }
}

extern "C" void nes_ref_pipeline_execute(void* pipeline, void* buffer, Output output, void* outputContext)
{
    auto& adapter = *static_cast<PipelineAdapter*>(pipeline);
    try
    {
        const std::scoped_lock lock(adapter.mutex);
        ReferenceExecutionContext context(adapter.buffers, adapter.id, output, outputContext);
        adapter.stage->execute(*static_cast<TupleBuffer*>(buffer), context);
    }
    catch (const std::exception& error)
    {
        NES_ERROR("Reference engine pipeline execution failed: {}", error.what());
        // TODO: reference-engine currently treats pipeline failures as a manager panic.
    }
    catch (...)
    {
        NES_ERROR("Reference engine pipeline execution failed with an unknown exception");
    }
}

extern "C" void nes_ref_pipeline_release(void* pipeline)
{
    delete static_cast<PipelineAdapter*>(pipeline);
}

extern "C" bool nes_ref_source_start(void* source, void* sender)
{
    try
    {
        return static_cast<SourceAdapter*>(source)->source->start(
            [sender](OriginId, SourceReturnType::SourceReturnType event, const std::stop_token&)
            {
                if (auto* data = std::get_if<SourceReturnType::Data>(&event))
                {
                    rq_source_data(sender, new TupleBuffer(data->buffer), data->buffer.getSequenceNumber().getRawValue());
                }
                else
                {
                    if (auto* error = std::get_if<SourceReturnType::Error>(&event))
                    {
                        NES_ERROR("Reference engine source failed: {}", error->ex.what());
                    }
                    rq_source_eos(sender);
                }
                return SourceReturnType::EmitResult::SUCCESS;
            });
    }
    catch (const std::exception& error)
    {
        NES_ERROR("Reference engine source start failed: {}", error.what());
        return false;
    }
    catch (...)
    {
        NES_ERROR("Reference engine source start failed with an unknown exception");
        return false;
    }
}

extern "C" void nes_ref_source_stop(void* source)
{
    try
    {
        static_cast<SourceAdapter*>(source)->source->stop();
    }
    catch (const std::exception& error)
    {
        NES_ERROR("Reference engine source stop failed: {}", error.what());
    }
    catch (...)
    {
        NES_ERROR("Reference engine source stop failed with an unknown exception");
    }
}

extern "C" void nes_ref_source_release(void* source)
{
    delete static_cast<SourceAdapter*>(source);
}

extern "C" void nes_ref_query_stopped(void* listener, size_t referenceId)
{
    auto& engine = *static_cast<ReferenceQueryEngine*>(listener);
    QueryId queryId = INVALID_QUERY_ID;
    {
        const std::scoped_lock lock(engine.referenceQueryMutex);
        if (const auto it = engine.referenceQueries.find(referenceId); it != engine.referenceQueries.end())
        {
            queryId = it->second;
            engine.referenceQueries.erase(it);
        }
    }
    if (queryId.isValid())
    {
        try
        {
            engine.statusListener->logQueryStatusChange(queryId, QueryStatus::Stopped, std::chrono::system_clock::now());
            engine.statisticListener->onEvent(QueryStop(WorkerThreadId(0), queryId));
        }
        catch (...)
        {
            // A C++ listener must not unwind through the Rust callback.
        }
    }
}

ReferenceQueryEngine::ReferenceQueryEngine(
    const QueryEngineConfiguration&,
    std::shared_ptr<QueryEngineStatisticListener> statListener,
    std::shared_ptr<AbstractQueryStatusListener> listener,
    std::shared_ptr<BufferManager> bm,
    const Host& host)
    : bufferManager(std::move(bm)), statusListener(std::move(listener)), statisticListener(std::move(statListener)), host(host)
{
    referenceManager = rq_manager_new(this);
}

ReferenceQueryEngine::~ReferenceQueryEngine()
{
    rq_manager_free(referenceManager);
}

void ReferenceQueryEngine::start(std::unique_ptr<ExecutableQueryPlan> plan)
{
    const auto referenceId = nextReferenceQueryId.fetch_add(1);
    const auto queryId = plan->queryId;
    {
        const std::scoped_lock lock(referenceQueryMutex);
        referenceQueries.emplace(referenceId, queryId);
    }
    void* rustPlan = nullptr;
    try
    {
        rustPlan = buildRustPlan(*plan, referenceId, bufferManager, statisticListener);
    }
    catch (...)
    {
        const std::scoped_lock lock(referenceQueryMutex);
        referenceQueries.erase(referenceId);
        throw;
    }

    statusListener->logQueryStatusChange(queryId, QueryStatus::Started, std::chrono::system_clock::now());
    if (rq_manager_start(referenceManager, rustPlan))
    {
        statusListener->logQueryStatusChange(queryId, QueryStatus::Running, std::chrono::system_clock::now());
        statisticListener->onEvent(QueryStart(WorkerThreadId(0), queryId));
    }
    else
    {
        {
            const std::scoped_lock lock(referenceQueryMutex);
            referenceQueries.erase(referenceId);
        }
        statusListener->logQueryFailure(queryId, NotImplemented("reference query manager rejected plan"), std::chrono::system_clock::now());
    }
}

bool ReferenceQueryEngine::adapt(
    std::unique_ptr<ExecutableQueryPlan> replacement, const std::vector<std::pair<PipelineId, PipelineId>>& stateTransfers)
{
    size_t referenceId = 0;
    {
        const std::scoped_lock lock(referenceQueryMutex);
        for (const auto& [id, original] : referenceQueries)
        {
            if (original == replacement->queryId)
            {
                referenceId = id;
                break;
            }
        }
    }
    PRECONDITION(referenceId != 0, "Cannot adapt a query that is not running");
    std::vector<size_t> donors;
    std::vector<size_t> targets;
    for (const auto& [donor, target] : stateTransfers)
    {
        donors.push_back(donor.getRawValue());
        targets.push_back(target.getRawValue());
    }
    return rq_manager_adapt(
        referenceManager,
        buildRustPlan(*replacement, referenceId, bufferManager, statisticListener),
        donors.data(),
        targets.data(),
        donors.size());
}

void ReferenceQueryEngine::stop(QueryId queryId)
{
    size_t referenceId = 0;
    {
        const std::scoped_lock lock(referenceQueryMutex);
        for (const auto& [id, original] : referenceQueries)
        {
            if (original == queryId)
            {
                referenceId = id;
                break;
            }
        }
    }
    if (referenceId != 0)
    {
        statisticListener->onEvent(QueryStopRequest(WorkerThreadId(0), queryId));
        static_cast<void>(rq_manager_stop(referenceManager, referenceId));
    }
}
}
