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
#include <cstring>
#include <exception>
#include <algorithm>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
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
void rq_plan_add_source(
    void* plan, size_t id, void* handle, const size_t* successors, size_t count, const char* sharingId, uint64_t startBytes, uint64_t nextSequence);
bool rq_manager_start(void* manager, void* plan);
void* rq_manager_pause(void* manager, size_t query, const size_t* pipelines, size_t count, size_t source);
void rq_snapshot_free(void* snapshot);
bool rq_snapshot_source_position(const void* snapshot, size_t donor, uint64_t* bytes, uint64_t* nextSequence);
size_t rq_plan_add_snapshot_source(void* plan, const void* snapshot, size_t donor);
bool rq_manager_start_from(void* manager, void* plan, const size_t* sources, const size_t* targets, size_t count);
bool rq_manager_adapt(void* manager, void* plan, const size_t* donors, const size_t* targets, size_t count);
bool rq_manager_stop(void* manager, size_t query);
void rq_source_data(void* sender, void* buffer, uint64_t sequence, uint64_t bytes);
void rq_source_eos(void* sender);
size_t rq_source_queued(void* sender);
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

    /// Only recorded: the pipeline stop retries itself (see ~PipelineAdapter); execute treats a repeat as a failure.
    void repeatTask(const TupleBuffer&, std::chrono::milliseconds delay) override
    {
        repeatDelay = delay;
        repeated = true;
    }

    bool repeated = false;
    std::chrono::milliseconds repeatDelay{0};

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
        std::shared_ptr<QueryEngineStatisticListener> statisticListener,
        ReferenceQueryEngine* engine,
        size_t referenceId)
        : stage(std::move(stage))
        , queryId(queryId)
        , id(id)
        , buffers(std::move(buffers))
        , statisticListener(std::move(statisticListener))
        , engine(engine)
        , referenceId(referenceId)
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
            /// A stage (e.g. the network sink flushing its channel) asks to be stopped again later via repeatTask.
            while (true)
            {
                ReferenceExecutionContext context(buffers, id, nullptr, nullptr);
                stage->stop(context);
                if (not context.repeated)
                {
                    break;
                }
                std::this_thread::sleep_for(std::max(context.repeatDelay, std::chrono::milliseconds(1)));
            }
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
    ReferenceQueryEngine* engine;
    size_t referenceId;
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
    /// Resume position: the source restarts from the beginning, drops `skipBytes` raw bytes, and renumbers the rest from `nextSequence`.
    /// ponytail: replays the prefix instead of seeking; add a seek offset to FileSource if large files make resume slow.
    uint64_t skipBytes = 0;
    std::optional<uint64_t> nextSequence;
};

void* buildRustPlan(
    ExecutableQueryPlan& plan,
    size_t referenceId,
    ReferenceQueryEngine* engine,
    const std::shared_ptr<BufferManager>& bufferManager,
    const std::shared_ptr<QueryEngineStatisticListener>& statisticListener,
    const ReferenceSourcePosition& position = {})
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
                std::move(pipeline->stage), plan.queryId, pipeline->id, bufferManager, statisticListener, engine, referenceId);
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
            adapter->skipBytes = position.bytes;
            if (position.nextSequence != SequenceNumber::INITIAL)
            {
                adapter->nextSequence = position.nextSequence;
            }
            rq_plan_add_source(
                rustPlan,
                sourceId.getRawValue(),
                adapter.release(),
                successors.data(),
                successors.size(),
                sharingId,
                position.bytes,
                position.nextSequence);
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

/// Returns false if the pipeline failed; the failure is kept for nes_ref_query_failed and the reference manager fails the query.
extern "C" bool nes_ref_pipeline_execute(void* pipeline, void* buffer, Output output, void* outputContext)
{
    auto& adapter = *static_cast<PipelineAdapter*>(pipeline);
    try
    {
        const std::scoped_lock lock(adapter.mutex);
        ReferenceExecutionContext context(adapter.buffers, adapter.id, output, outputContext);
        adapter.stage->execute(*static_cast<TupleBuffer*>(buffer), context);
        if (context.repeated)
        {
            throw NotImplemented("repeatTask during execute is unavailable in the reference query engine prototype");
        }
        return true;
    }
    catch (...)
    {
        auto failure = wrapExternalException();
        NES_ERROR("Reference engine pipeline execution failed: {}", failure.what());
        const std::scoped_lock lock(adapter.engine->referenceQueryMutex);
        adapter.engine->referenceFailures.try_emplace(adapter.referenceId, std::move(failure));
        return false;
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
        auto* adapter = static_cast<SourceAdapter*>(source);
        return adapter->source->start(
            [sender, adapter](OriginId, SourceReturnType::SourceReturnType event, const std::stop_token& stopToken)
            {
                if (auto* data = std::get_if<SourceReturnType::Data>(&event))
                {
                    /// Backpressure: wait while too many of this source's buffers are queued but not yet polled. Stopping ends the wait.
                    /// ponytail: fixed bound; use the source's configured max inflight buffers once SourceHandle exposes it.
                    constexpr size_t maxQueuedBuffers = 64;
                    while (rq_source_queued(sender) >= maxQueuedBuffers && not stopToken.stop_requested())
                    {
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                    /// Raw source buffers carry their byte count as number of tuples.
                    auto& buffer = data->buffer;
                    if (const auto bytes = buffer.getNumberOfTuples(); adapter->skipBytes > 0 && bytes > 0)
                    {
                        if (bytes <= adapter->skipBytes)
                        {
                            adapter->skipBytes -= bytes;
                            return SourceReturnType::EmitResult::SUCCESS;
                        }
                        auto memory = buffer.getAvailableMemoryArea<std::byte>();
                        std::memmove(memory.data(), memory.data() + adapter->skipBytes, bytes - adapter->skipBytes);
                        buffer.setNumberOfTuples(bytes - adapter->skipBytes);
                        adapter->skipBytes = 0;
                    }
                    if (adapter->nextSequence)
                    {
                        buffer.setSequenceNumber(SequenceNumber((*adapter->nextSequence)++));
                    }
                    rq_source_data(sender, new TupleBuffer(buffer), buffer.getSequenceNumber().getRawValue(), buffer.getNumberOfTuples());
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

extern "C" void nes_ref_query_failed(void* listener, size_t referenceId)
{
    auto& engine = *static_cast<ReferenceQueryEngine*>(listener);
    QueryId queryId = INVALID_QUERY_ID;
    std::optional<Exception> failure;
    {
        const std::scoped_lock lock(engine.referenceQueryMutex);
        if (const auto it = engine.referenceQueries.find(referenceId); it != engine.referenceQueries.end())
        {
            queryId = it->second;
            engine.referenceQueries.erase(it);
        }
        if (const auto it = engine.referenceFailures.find(referenceId); it != engine.referenceFailures.end())
        {
            failure = std::move(it->second);
            engine.referenceFailures.erase(it);
        }
    }
    if (queryId.isValid())
    {
        try
        {
            engine.statusListener->logQueryFailure(
                queryId, failure.value_or(NotImplemented("reference query failed")), std::chrono::system_clock::now());
            engine.statisticListener->onEvent(QueryFail(WorkerThreadId(0), queryId));
        }
        catch (...)
        {
            // A C++ listener must not unwind through the Rust callback.
        }
    }
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

namespace
{
ReferenceQueryShape shapeOf(const ExecutableQueryPlan& plan)
{
    ReferenceQueryShape shape;
    for (const auto& pipeline : plan.pipelines)
    {
        shape.pipelines.push_back(pipeline->id.getRawValue());
    }
    for (const auto& [source, successors] : plan.sources)
    {
        shape.sources.push_back(source->getSourceId().getRawValue());
    }
    return shape;
}
}

ReferenceSnapshot::~ReferenceSnapshot()
{
    rq_snapshot_free(handle);
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
        referenceShapes.insert_or_assign(referenceId, shapeOf(*plan));
    }
    void* rustPlan = nullptr;
    try
    {
        rustPlan = buildRustPlan(*plan, referenceId, this, bufferManager, statisticListener);
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
    {
        const std::scoped_lock lock(referenceQueryMutex);
        referenceShapes.insert_or_assign(referenceId, shapeOf(*replacement));
    }
    std::vector<size_t> donors;
    std::vector<size_t> targets;
    for (const auto& [donor, target] : stateTransfers)
    {
        donors.push_back(donor.getRawValue());
        targets.push_back(target.getRawValue());
    }
    return rq_manager_adapt(
        referenceManager,
        buildRustPlan(*replacement, referenceId, this, bufferManager, statisticListener),
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

std::unique_ptr<ReferenceSnapshot> ReferenceQueryEngine::pause(QueryId queryId)
{
    size_t referenceId = 0;
    ReferenceQueryShape shape;
    {
        const std::scoped_lock lock(referenceQueryMutex);
        for (const auto& [id, original] : referenceQueries)
        {
            if (original == queryId)
            {
                referenceId = id;
                shape = referenceShapes.at(id);
                referenceShapes.erase(id);
                break;
            }
        }
    }
    PRECONDITION(referenceId != 0, "Cannot pause a query that is not running");
    PRECONDITION(shape.sources.size() == 1, "Pausing supports exactly one source");
    /// Returns once the snapshot is complete; the query then reports Stopped.
    void* handle = rq_manager_pause(referenceManager, referenceId, shape.pipelines.data(), shape.pipelines.size(), shape.sources.front());
    if (handle == nullptr)
    {
        throw NotImplemented("reference query manager could not pause the query");
    }
    auto snapshot = std::make_unique<ReferenceSnapshot>();
    snapshot->handle = handle;
    snapshot->pipelines = shape.pipelines.size();
    return snapshot;
}

void ReferenceQueryEngine::resume(std::unique_ptr<ExecutableQueryPlan> plan, const ReferenceSnapshot& snapshot)
{
    auto shape = shapeOf(*plan);
    PRECONDITION(shape.sources.size() == 1, "Resuming supports exactly one source");
    PRECONDITION(shape.pipelines.size() == snapshot.pipelines, "Resumed plan must have the paused plan's pipelines, paired by position");
    ReferenceSourcePosition position;
    if (not rq_snapshot_source_position(snapshot.handle, snapshot.pipelines, &position.bytes, &position.nextSequence))
    {
        throw NotImplemented("snapshot does not contain a source position");
    }
    const auto referenceId = nextReferenceQueryId.fetch_add(1);
    const auto queryId = plan->queryId;
    {
        const std::scoped_lock lock(referenceQueryMutex);
        referenceQueries.emplace(referenceId, queryId);
        referenceShapes.insert_or_assign(referenceId, shape);
    }
    void* rustPlan = nullptr;
    try
    {
        rustPlan = buildRustPlan(*plan, referenceId, this, bufferManager, statisticListener, position);
    }
    catch (...)
    {
        const std::scoped_lock lock(referenceQueryMutex);
        referenceQueries.erase(referenceId);
        referenceShapes.erase(referenceId);
        throw;
    }
    std::vector<size_t> donors;
    for (size_t donor = 0; donor < shape.pipelines.size(); ++donor)
    {
        donors.push_back(rq_plan_add_snapshot_source(rustPlan, snapshot.handle, donor));
    }

    statusListener->logQueryStatusChange(queryId, QueryStatus::Started, std::chrono::system_clock::now());
    if (rq_manager_start_from(referenceManager, rustPlan, donors.data(), shape.pipelines.data(), donors.size()))
    {
        statusListener->logQueryStatusChange(queryId, QueryStatus::Running, std::chrono::system_clock::now());
        statisticListener->onEvent(QueryStart(WorkerThreadId(0), queryId));
    }
    else
    {
        {
            const std::scoped_lock lock(referenceQueryMutex);
            referenceQueries.erase(referenceId);
            referenceShapes.erase(referenceId);
        }
        statusListener->logQueryFailure(queryId, NotImplemented("reference query manager rejected resume"), std::chrono::system_clock::now());
    }
}
}
