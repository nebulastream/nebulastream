//
// Created by Lukas Schwerdtfeger on 12.07.26.
//

#include <atomic>
#include <chrono>
#include <compare>
#include <condition_variable>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <print>
#include <queue>
#include <ranges>
#include <set>
#include <thread>
#include <variant>
#include <vector>

#define LOG(level, ...) std::println("[{}] {}", level, std::format(__VA_ARGS__))
#define LOG_ERROR(...) LOG("E", __VA_ARGS__)
#define LOG_DEBUG(...) LOG("D", __VA_ARGS__)
#define LOG_DEBUG(...)

template <typename T>
struct Mutex
{
    struct Guard
    {
        std::unique_lock<std::mutex> lk;
        T& t;

        Guard(std::unique_lock<std::mutex> lk, T& t) : lk(std::move(lk)), t(t) { }

        Guard(const Guard& other) = delete;
        Guard(Guard&& other) noexcept = delete;
        Guard& operator=(const Guard& other) = delete;
        Guard& operator=(Guard&& other) noexcept = delete;

        T& operator*() { return t; }

        T* operator->() { return &t; }

        ~Guard() { lk.unlock(); }
    };

    std::mutex mtx;
    T t;

    explicit Mutex(T t) : t(std::move(t)) { }

    Mutex() = default;

    Guard lock()
    {
        std::unique_lock<std::mutex> lk(mtx);
        return Guard(std::move(lk), t);
    }
};

struct PipelineAbsorbResult
{
};

struct PipelineEmitResult
{
};

struct PipelineExecutionResult
{
};

struct Sequence
{
    size_t sequence;
    auto operator<=>(const Sequence&) const = default;

    Sequence next() const { return Sequence{sequence + 1}; }

    Sequence prev() const { return Sequence{sequence - 1}; }
};

struct Buffer
{
    Sequence sequence;
};

struct PipelineExecutionContext
{
    virtual Buffer allocate() = 0;
    virtual void emit(Buffer) = 0;

protected:
    ~PipelineExecutionContext() = default;
};

struct Pipeline
{
    virtual ~Pipeline() = default;
    virtual PipelineAbsorbResult absorb(const Buffer&, PipelineExecutionContext&) = 0;
    virtual PipelineEmitResult emit(PipelineExecutionContext&) = 0;

    virtual PipelineExecutionResult execute(const Buffer&, PipelineExecutionContext&) = 0;
};

struct PipelineNodeId
{
    size_t id;
    auto operator<=>(const PipelineNodeId&) const = default;
};

struct PipelineNode;

struct Version
{
    size_t v;
};

struct Lifetime
{
    Version aliveSince;
    std::optional<Version> aliveUntil;

    bool isAlive(Version v)
    {
        if (aliveSince.v > v.v)
        {
            return false;
        }
        if (aliveUntil.has_value() && aliveUntil->v < v.v)
        {
            return false;
        }
        return true;
    }
};

struct PipelineNode
{
    std::shared_ptr<Pipeline> pipeline;
    std::vector<PipelineNodeId> successor;
    std::vector<PipelineNodeId> predecessor;
};

PipelineNodeId generatePipelineNodeId()
{
    static std::atomic_size_t pipelineIdGenerator = 1;
    return PipelineNodeId{pipelineIdGenerator.fetch_add(1)};
}

struct PipelineGraph
{
    Version version{0};
    std::map<PipelineNodeId, PipelineNode> nodes;
};

struct Graph
{
    std::shared_ptr<const PipelineGraph> current;
    std::shared_ptr<PipelineGraph> pending;
    std::mutex lifecycleMtx;
    std::condition_variable_any lifecycleCv;

    Graph() : current(std::make_shared<const PipelineGraph>()) { }

    std::shared_ptr<const PipelineGraph> snapshot() const
    {
        return std::atomic_load_explicit(&current, std::memory_order_acquire);
    }
};

struct VersionTransition
{
    std::mutex mtx;
    std::condition_variable_any completed;
    size_t remaining;

    explicit VersionTransition(size_t remaining) : remaining(remaining) { }

    void complete()
    {
        {
            std::unique_lock lock(mtx);
            --remaining;
        }
        completed.notify_all();
    }

    bool wait(std::stop_token token)
    {
        std::unique_lock lock(mtx);
        return completed.wait(lock, token, [&]() { return remaining == 0; });
    }
};

struct ExecutePipelineTask
{
    std::shared_ptr<const PipelineGraph> graph;
    PipelineNodeId id;
    Buffer data;
};

struct StartPipelineTask
{
    std::shared_ptr<PipelineGraph> graph;
    PipelineNodeId id;
    Version version;
    std::shared_ptr<VersionTransition> transition;
};

using Task = std::variant<ExecutePipelineTask, StartPipelineTask>;

template <typename T>
struct Queue
{
    std::mutex mtx;
    std::condition_variable_any cv;
    std::deque<T> queue{};

    std::optional<T> pop_blocking(std::stop_token token)
    {
        std::unique_lock<std::mutex> lk(mtx);
        if (queue.empty())
        {
            cv.wait(lk, token, [&]() { return !queue.empty() || token.stop_requested(); });
            if (token.stop_requested())
            {
                return std::nullopt;
            }
        }
        T ret = std::move(queue.front());
        queue.pop_front();
        return ret;
    }

    void push(T&& t)
    {
        {
            std::unique_lock<std::mutex> lk(mtx);
            queue.push_back(std::forward<T>(t));
        }
        cv.notify_all();
    }
};

struct WorkerThread
{
    void executePipeline(const ExecutePipelineTask& task)
    {
        LOG_DEBUG("Executing pipeline {} with sequence {}", task.id.id, task.data.sequence.sequence);

        struct TaskExecutionContext : PipelineExecutionContext
        {
            std::vector<Buffer> buffers;

            Buffer allocate() override { return Buffer{}; }

            void emit(Buffer b) override { buffers.emplace_back(std::move(b)); }
        };

        TaskExecutionContext tec;
        auto node = task.graph->nodes.find(task.id);
        if (node == task.graph->nodes.end())
        {
            LOG_ERROR("Pipeline {} does not exist in graph version {}", task.id.id, task.graph->version.v);
            return;
        }

        node->second.pipeline->execute(task.data, tec);
        LOG_DEBUG("Pipeline {} produced {} buffer(s)", task.id.id, tec.buffers.size());
        LOG_DEBUG("Routing output from pipeline {} at graph version {}", task.id.id, task.graph->version.v);
        LOG_DEBUG("Pipeline {} has {} successor(s)", task.id.id, node->second.successor.size());
        for (const auto& buffer : std::move(tec.buffers))
        {
            for (auto successor : node->second.successor)
            {
                LOG_DEBUG("Scheduling sequence {} for pipeline {}", buffer.sequence.sequence, successor.id);
                queue.push(Task{ExecutePipelineTask{
                    .graph = task.graph,
                    .id = successor,
                    .data = buffer}});
            }
        }
    }

    void startPipeline(const StartPipelineTask& task)
    {
        LOG_DEBUG("Worker starting pipeline {} for version {}", task.id.id, task.version.v);

        struct InitialExecutionContext final : PipelineExecutionContext
        {
            Buffer allocate() override { return Buffer{}; }
            void emit(Buffer) override { }
        };

        InitialExecutionContext context;
        task.graph->nodes.at(task.id).pipeline->absorb(Buffer{.sequence = Sequence{0}}, context);
        task.transition->complete();
        LOG_DEBUG("Pipeline {} started for version {}", task.id.id, task.version.v);
    }

    void execute(const Task& task)
    {
        std::visit(
            [this](const auto& concreteTask)
            {
                using ConcreteTask = std::decay_t<decltype(concreteTask)>;
                if constexpr (std::same_as<ConcreteTask, ExecutePipelineTask>)
                {
                    executePipeline(concreteTask);
                }
                else
                {
                    startPipeline(concreteTask);
                }
            },
            task);
    }

    void run(std::stop_token token)
    {
        while (auto task = queue.pop_blocking(token))
        {
            std::visit(
                [](const auto& concreteTask)
                {
                    using ConcreteTask = std::decay_t<decltype(concreteTask)>;
                    if constexpr (std::same_as<ConcreteTask, ExecutePipelineTask>)
                    {
                        LOG_DEBUG(
                            "Worker received execution task: pipeline={}, sequence={}",
                            concreteTask.id.id,
                            concreteTask.data.sequence.sequence);
                    }
                    else
                    {
                        LOG_DEBUG(
                            "Worker received startup task: pipeline={}, version={}",
                            concreteTask.id.id,
                            concreteTask.version.v);
                    }
                },
                *task);
            execute(*task);
        }
    }

    WorkerThread(Queue<Task>& queue, Graph& graph) : queue(queue), graph(graph)
    {
        thread = std::jthread([](std::stop_token stoken, WorkerThread* self) { self->run(stoken); }, this);
    }

    Queue<Task>& queue;
    Graph& graph;
    std::jthread thread;
};

struct LifeCycleThread
{
    void run(std::stop_token token)
    {
        LOG_DEBUG("Lifecycle thread started");
        while (!token.stop_requested())
        {
            std::shared_ptr<PipelineGraph> pending;
            auto current = graph.snapshot();
            {
                std::unique_lock lock(graph.lifecycleMtx);
                LOG_DEBUG(
                    "Waiting for graph changes; current version={}", current->version.v);
                if (!graph.lifecycleCv.wait(lock, token, [&]() { return graph.pending != nullptr; }))
                {
                    LOG_DEBUG("Lifecycle thread stopping");
                    return;
                }
                pending = graph.pending;
            }

            std::vector<PipelineNodeId> pipelinesToStart;
            for (const auto& [id, node] : pending->nodes)
            {
                if (!current->nodes.contains(id))
                {
                    pipelinesToStart.emplace_back(id);
                }
            }

            LOG_DEBUG("Starting graph version {} with {} new pipeline(s)", pending->version.v, pipelinesToStart.size());
            auto transition = std::make_shared<VersionTransition>(pipelinesToStart.size());
            for (auto id : pipelinesToStart)
            {
                LOG_DEBUG("Scheduling pipeline {} for version {}", id.id, pending->version.v);
                queue.push(Task{StartPipelineTask{
                    .graph = pending,
                    .id = id,
                    .version = pending->version,
                    .transition = transition}});
            }

            if (!transition->wait(token))
            {
                LOG_DEBUG("Lifecycle thread stopped while starting version {}", pending->version.v);
                return;
            }

            {
                std::unique_lock lock(graph.lifecycleMtx);
                std::atomic_store_explicit(
                    &graph.current, std::shared_ptr<const PipelineGraph>{pending}, std::memory_order_release);
                graph.pending.reset();
            }
            LOG_DEBUG("Activated graph version {}", pending->version.v);
            graph.lifecycleCv.notify_all();
        }
    }

    LifeCycleThread(Queue<Task>& queue, Graph& graph) : queue(queue), graph(graph)
    {
        thread = std::jthread([](std::stop_token stoken, LifeCycleThread* self) { self->run(stoken); }, this);
    }

    Queue<Task>& queue;
    Graph& graph;
    std::jthread thread;
};

struct Source
{
    void runningRoutine(std::stop_token token)
    {
        Sequence sequence{.sequence = 1};
        while (!token.stop_requested())
        {
            auto buffer = generator();
            buffer.sequence = sequence;
            sequence = sequence.next();
            auto graphSnapshot = graph.snapshot();
            for (auto successor : successors)
            {
                queue.push(Task{ExecutePipelineTask{.graph = graphSnapshot, .id = successor, .data = buffer}});
            }
        }
    }

    Source(std::function<Buffer()> generator, std::vector<PipelineNodeId> successors, Queue<Task>& queue, Graph& graph)
        : generator(std::move(generator)), queue(queue), graph(graph), successors(std::move(successors))
    {
        thread = std::jthread([](std::stop_token stoken, Source* self) { self->runningRoutine(stoken); }, this);
    }

    std::function<Buffer()> generator;
    Queue<Task>& queue;
    Graph& graph;
    std::vector<PipelineNodeId> successors;
    std::jthread thread;
};

struct Engine
{
    Queue<Task> queue{};
    Graph graph;
    std::vector<std::unique_ptr<WorkerThread>> threads;
    std::unique_ptr<LifeCycleThread> lifecycleThread;
    std::vector<std::unique_ptr<Source>> sources;

    Engine()
    {
        threads.reserve(4);
        threads.emplace_back(std::make_unique<WorkerThread>(queue, graph));
        threads.emplace_back(std::make_unique<WorkerThread>(queue, graph));
        threads.emplace_back(std::make_unique<WorkerThread>(queue, graph));
        lifecycleThread = std::make_unique<LifeCycleThread>(queue, graph);
        threads.emplace_back(std::make_unique<WorkerThread>(queue, graph));
    }

    void startSource(std::function<Buffer()> generator, std::vector<PipelineNodeId> successors)
    {
        sources.emplace_back(std::make_unique<Source>(generator, successors, queue, graph));
    }
};

namespace impl
{

struct CompletionTracker
{
    struct State
    {
        std::set<Sequence> sequences;
        Sequence minSequence;
        Sequence maxSequence;
    };

    Mutex<State> stateMtx;

    explicit CompletionTracker(Sequence start) : stateMtx(State{.sequences = {}, .minSequence = start, .maxSequence = start}) { }

    Sequence minSequence()
    {
        auto state = stateMtx.lock();
        return state->minSequence;
    }

    Sequence complete(Sequence sequence)
    {
        auto state = stateMtx.lock();
        if (state->minSequence.next() == sequence)
        {
            state->minSequence = sequence;
            while (state->sequences.contains(state->minSequence.next()))
            {
                state->sequences.erase(state->minSequence.next());
                state->minSequence = state->minSequence.next();
            }
        }
        if (sequence > state->maxSequence)
        {
            state->maxSequence = sequence;
        }

        return state->minSequence;
    }
};

struct SinkPipeline final : Pipeline
{
    struct State
    {
        std::unique_ptr<CompletionTracker> tracker;
    };

    std::string name;
    Mutex<State> state;

    explicit SinkPipeline(const std::string& name) : name(name) { }

    PipelineAbsorbResult absorb(const Buffer& buffer, PipelineExecutionContext&) override
    {
        auto locked = state.lock();
        locked->tracker = std::make_unique<CompletionTracker>(buffer.sequence);
        return {};
    }

    PipelineEmitResult emit(PipelineExecutionContext& pec) override
    {
        auto buffer = pec.allocate();
        auto locked = state.lock();
        buffer.sequence = locked->tracker->minSequence();

        return {};
    }

    PipelineExecutionResult execute(const Buffer& buffer, PipelineExecutionContext&) override
    {
        std::println(std::cerr, "{}: {}", name, buffer.sequence.sequence);
        return {};
    }
};

struct PassThroughPipeline final : Pipeline
{
    struct State
    {
        std::unique_ptr<CompletionTracker> tracker;
    };

    Mutex<State> state;

    PipelineAbsorbResult absorb(const Buffer& buffer, PipelineExecutionContext&) override
    {
        auto locked = state.lock();
        locked->tracker = std::make_unique<CompletionTracker>(buffer.sequence);
        return {};
    }

    PipelineEmitResult emit(PipelineExecutionContext& pec) override
    {
        auto buffer = pec.allocate();
        auto locked = state.lock();
        buffer.sequence = locked->tracker->minSequence();

        return {};
    }

    PipelineExecutionResult execute(const Buffer& buffer, PipelineExecutionContext& pec) override
    {
        auto locked = state.lock();
        locked->tracker->complete(buffer.sequence);
        auto outputBuffer = pec.allocate();
        outputBuffer.sequence = buffer.sequence;
        pec.emit(outputBuffer);
        return {};
    }
};
}

struct GraphInterface
{
    Graph& graph;

    struct VersionedPipelineNodeId
    {
        Version version;
        PipelineNodeId pipeline;
    };

    struct TransactionHandle
    {
        std::shared_ptr<PipelineGraph> workingGraph;
        GraphInterface& owner;
        size_t changeCount = 0;
        bool pending = true;

        TransactionHandle(std::shared_ptr<PipelineGraph> workingGraph, GraphInterface& owner)
            : workingGraph(std::move(workingGraph)), owner(owner)
        {
        }

        TransactionHandle(const TransactionHandle& other) = delete;
        TransactionHandle(TransactionHandle&& other) noexcept = delete;
        TransactionHandle& operator=(const TransactionHandle& other) = delete;
        TransactionHandle& operator=(TransactionHandle&& other) noexcept = delete;

        ~TransactionHandle()
        {
            if (pending)
            {
                owner.abort(*this);
            }
        }

        void commit() &&
        {
            pending = false;
            owner.commit(std::move(*this));
        }

        template <typename PipelineType, typename... Args>
        VersionedPipelineNodeId addPipeline(Args&&... args)
        {
            auto pipelineId = VersionedPipelineNodeId{workingGraph->version, generatePipelineNodeId()};
            workingGraph->nodes.emplace(
                pipelineId.pipeline,
                PipelineNode{
                    .pipeline = std::make_shared<PipelineType>(std::forward<Args>(args)...),
                    .successor = {},
                    .predecessor = {}});
            ++changeCount;
            LOG_DEBUG("Transaction {}: added pipeline {}", workingGraph->version.v, pipelineId.pipeline.id);
            return pipelineId;
        }
        void removePipeline(VersionedPipelineNodeId pipelineId)
        {
            LOG_DEBUG("Transaction {}: removing pipeline {}", workingGraph->version.v, pipelineId.pipeline.id);
            workingGraph->nodes.erase(pipelineId.pipeline);
            for (auto& [id, node] : workingGraph->nodes)
            {
                std::erase(node.successor, pipelineId.pipeline);
                std::erase(node.predecessor, pipelineId.pipeline);
            }
            ++changeCount;
        }
        void connect(VersionedPipelineNodeId from, VersionedPipelineNodeId to)
        {
            LOG_DEBUG("Transaction {}: connecting pipeline {} -> {}", workingGraph->version.v, from.pipeline.id, to.pipeline.id);
            workingGraph->nodes.at(from.pipeline).successor.emplace_back(to.pipeline);
            workingGraph->nodes.at(to.pipeline).predecessor.emplace_back(from.pipeline);
            ++changeCount;
        }
    };

    TransactionHandle snapshot()
    {
        std::unique_lock lock(graph.lifecycleMtx);
        graph.lifecycleCv.wait(lock, [&]() { return graph.pending == nullptr; });
        auto current = graph.snapshot();
        auto workingGraph = std::make_shared<PipelineGraph>(*current);
        workingGraph->version = Version{current->version.v + 1};
        LOG_DEBUG("Opened transaction for version {}; current={}", workingGraph->version.v, current->version.v);
        return TransactionHandle{std::move(workingGraph), *this};
    }

    /// Lookup a pipeline node id at a given version. If the pipeline is alive at that version number we return a VersionedPipelineNodeId.
    /// VersionPipelineNodes are used through the snapshot interface.
    std::optional<VersionedPipelineNodeId> lookup(Version version, PipelineNodeId id)
    {
        auto current = graph.snapshot();
        if (current->version.v != version.v || !current->nodes.contains(id))
        {
            return std::nullopt;
        }
        return VersionedPipelineNodeId{version, id};
    }

    void commit(TransactionHandle&& handle)
    {
        auto version = handle.workingGraph->version;
        LOG_DEBUG("Transaction {}: committing {} change(s)", version.v, handle.changeCount);
        std::unique_lock lock(graph.lifecycleMtx);
        graph.pending = std::move(handle.workingGraph);
        LOG_DEBUG("Transaction {}: published; waiting for activation", version.v);
        graph.lifecycleCv.notify_all();
        graph.lifecycleCv.wait(lock, [&]() { return graph.snapshot()->version.v >= version.v; });
        LOG_DEBUG("Transaction {}: activated", version.v);
    }

    void abort(TransactionHandle& handle)
    {
        LOG_DEBUG(
            "Transaction {}: aborting {} pending change(s)", handle.workingGraph->version.v, handle.changeCount);
        handle.workingGraph.reset();
        handle.pending = false;
    }
};

int main()
{
    Engine engine;
    GraphInterface graphInterface{engine.graph};

    auto transaction = graphInterface.snapshot();
    auto p1 = transaction.addPipeline<impl::PassThroughPipeline>();
    auto sink1 = transaction.addPipeline<impl::SinkPipeline>("Sink1");
    transaction.connect(p1, sink1);
    std::move(transaction).commit();

    engine.startSource(
        std::function<Buffer()>(
            []()
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                return Buffer{};
            }),
        std::vector<PipelineNodeId>{p1.pipeline});

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    {
        auto t1 = graphInterface.snapshot();
        auto s2 = t1.addPipeline<impl::SinkPipeline>("Sink2");
        t1.connect(p1, s2);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::move(t1).commit();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    {
        auto t1 = graphInterface.snapshot();
        t1.removePipeline(sink1);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::move(t1).commit();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}
