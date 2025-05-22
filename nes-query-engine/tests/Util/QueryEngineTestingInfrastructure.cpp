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

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <ostream>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Overloaded.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineTestingInfrastructure.hpp>
#include <TestSource.hpp>

namespace NES::Testing
{

std::vector<std::byte> identifiableData(size_t identifier)
{
    std::vector<std::byte> data(DEFAULT_BUFFER_SIZE);
    const size_t stepSize = sizeof(identifier) / sizeof(std::byte);
    for (size_t index = 0; index < data.size() / stepSize; index += stepSize)
    {
        *std::bit_cast<size_t*>(&data[stepSize]) = identifier;
    }

    return data;
}

bool verifyIdentifier(const Memory::TupleBuffer& buffer, size_t identifier)
{
    if (buffer.getBufferSize() == 0)
    {
        return false;
    }

    const size_t stepSize = sizeof(identifier) / sizeof(std::byte);
    bool allMatch = true;
    for (size_t index = 0; index < buffer.getBufferSize() / stepSize; index += stepSize)
    {
        allMatch |= *std::bit_cast<size_t*>(&buffer.getBuffer<std::byte>()[stepSize]) == identifier;
    }

    return allMatch;
}

std::ostream& TestPipeline::toString(std::ostream& os) const
{
    return os << "TestPipeline";
}

testing::AssertionResult TestSinkController::waitForNumberOfReceivedBuffersOrMore(size_t numberOfExpectedBuffers)
{
    auto buffers = receivedBuffers.lock();
    if (buffers->size() >= numberOfExpectedBuffers)
    {
        return testing::AssertionSuccess();
    }

    auto check = receivedBufferTrigger.wait_for(
        buffers.as_lock(), DEFAULT_LONG_AWAIT_TIMEOUT, [&]() { return buffers->size() >= numberOfExpectedBuffers; });

    if (check)
    {
        return testing::AssertionSuccess();
    }

    return testing::AssertionFailure() << fmt::format(
               "The expected number of TupleBuffers were not received after {}. Expected: {}, but Received {}",
               std::chrono::duration_cast<std::chrono::milliseconds>(DEFAULT_LONG_AWAIT_TIMEOUT),
               numberOfExpectedBuffers,
               buffers->size());
}

void TestSinkController::insertBuffer(Memory::TupleBuffer&& buffer)
{
    ++invocations;
    receivedBuffers.lock()->push_back(std::move(buffer));
    receivedBufferTrigger.notify_one();
}

std::vector<Memory::TupleBuffer> TestSinkController::takeBuffers()
{
    return receivedBuffers.exchange({});
}

std::ostream& TestSink::toString(std::ostream& os) const
{
    return os << "TestSink";
}

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestSinkController>>
createSinkPipeline(PipelineId id, std::shared_ptr<Memory::AbstractBufferProvider> bm)
{
    auto sinkController = std::make_shared<TestSinkController>();
    auto stage = std::make_unique<TestSink>(std::move(bm), sinkController);
    auto pipeline = Runtime::Execution::ExecutablePipeline::create(id, std::move(stage), {});
    return {pipeline, sinkController};
}

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestPipelineController>>
createPipeline(PipelineId id, const std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>& successors)
{
    auto pipelineCtrl = std::make_shared<TestPipelineController>();
    auto stage = std::make_unique<TestPipeline>(pipelineCtrl);
    auto pipeline = Runtime::Execution::ExecutablePipeline::create(id, std::move(stage), successors);
    return {pipeline, pipelineCtrl};
}

QueryPlanBuilder::identifier_t QueryPlanBuilder::addPipeline(const std::vector<identifier_t>& predecssors)
{
    auto identifier = nextIdentifier++;
    for (auto pred : predecssors)
    {
        INVARIANT(!std::holds_alternative<SinkDescriptor>(objects[pred]), "Sink Descriptor cannot be a predecessor");
        forwardRelations[pred].push_back(identifier);
        backwardRelations[identifier].push_back(pred);
    }

    objects[identifier] = PipelineDescriptor{PipelineId(pipelineIdCounter++)};
    return identifier;
}

QueryPlanBuilder::identifier_t QueryPlanBuilder::addSource()
{
    auto identifier = nextIdentifier++;
    objects[identifier] = SourceDescriptor{OriginId(originIdCounter++)};
    forwardRelations[identifier] = {};
    return identifier;
}

QueryPlanBuilder::identifier_t QueryPlanBuilder::addSink(const std::vector<identifier_t>& predecessors)
{
    auto identifier = nextIdentifier++;
    for (auto pred : predecessors)
    {
        assert(!std::holds_alternative<SinkDescriptor>(objects[pred]) && "Sink Descriptor cannot be a predecessor");
        forwardRelations[pred].push_back(identifier);
        backwardRelations[identifier].push_back(pred);
    }

    objects[identifier] = SinkDescriptor{PipelineId(pipelineIdCounter++)};
    return identifier;
}

QueryPlanBuilder::TestPlanCtrl QueryPlanBuilder::build(QueryId queryId, std::shared_ptr<Memory::BufferManager> bm) &&
{
    auto isSource = std::ranges::views::filter([](const std::pair<identifier_t, QueryComponentDescriptor>& kv)
                                               { return std::holds_alternative<SourceDescriptor>(kv.second); });
    std::vector<std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>>>>
        sources;

    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> pipelines;
    std::unordered_map<identifier_t, OriginId> sourceIds;
    std::unordered_map<identifier_t, PipelineId> pipelineIds;

    std::unordered_map<identifier_t, Runtime::Execution::ExecutablePipelineStage*> stages;
    std::unordered_map<identifier_t, std::shared_ptr<Sources::TestSourceControl>> sourceCtrls;
    std::unordered_map<identifier_t, std::shared_ptr<TestSinkController>> sinkCtrls;
    std::unordered_map<identifier_t, std::shared_ptr<TestPipelineController>> pipelineCtrls;
    std::unordered_map<identifier_t, std::shared_ptr<Runtime::Execution::ExecutablePipeline>> cache{};
    std::function<std::shared_ptr<Runtime::Execution::ExecutablePipeline>(identifier_t)> getOrCreatePipeline = [&](identifier_t identifier)
    {
        if (auto it = cache.find(identifier); it != cache.end())
        {
            return it->second;
        }

        auto result = std::visit(
            Overloaded{
                [](SourceDescriptor) -> std::shared_ptr<Runtime::Execution::ExecutablePipeline>
                {
                    INVARIANT(false, "Source cannot be a successor");
                    std::terminate(); /// Ensures termination if INVARIANT is a no-op in release mode.
                },
                [&](SinkDescriptor descriptor) -> std::shared_ptr<Runtime::Execution::ExecutablePipeline>
                {
                    auto [sink, ctrl] = createSinkPipeline(descriptor.pipelineId, bm);
                    pipelines.push_back(sink);
                    stages[identifier] = sink->stage.get();
                    sinkCtrls[identifier] = ctrl;
                    pipelineIds.emplace(identifier, descriptor.pipelineId);
                    return pipelines.back();
                },
                [&](PipelineDescriptor descriptor) -> std::shared_ptr<Runtime::Execution::ExecutablePipeline>
                {
                    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> successors;
                    std::ranges::transform(forwardRelations.at(identifier), std::back_inserter(successors), getOrCreatePipeline);
                    auto [pipeline, pipelineCtrl] = createPipeline(descriptor.pipelineId, successors);
                    stages[identifier] = pipeline->stage.get();
                    pipelines.push_back(std::move(pipeline));
                    pipelineIds.emplace(identifier, descriptor.pipelineId);
                    pipelineCtrls[identifier] = pipelineCtrl;
                    return pipelines.back();
                }},
            objects[identifier]);

        cache[identifier] = result;
        return result;
    };

    for (auto source : objects | isSource)
    {
        std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>> successors;
        std::ranges::transform(forwardRelations.at(source.first), std::back_inserter(successors), getOrCreatePipeline);
        auto [s, ctrl] = Sources::getTestSource(std::get<SourceDescriptor>(source.second).sourceId, bm);
        sourceIds.emplace(source.first, s->getSourceId());
        sources.emplace_back(std::move(s), std::move(successors));
        sourceCtrls[source.first] = ctrl;
    }

    return {
        .query = std::make_unique<Runtime::ExecutableQueryPlan>(queryId, std::move(pipelines), std::move(sources)),
        .sourceIds = sourceIds,
        .pipelineIds = pipelineIds,
        .sourceCtrls = sourceCtrls,
        .sinkCtrls = sinkCtrls,
        .pipelineCtrls = pipelineCtrls,
        .stages = stages};
}

QueryPlanBuilder::QueryPlanBuilder(
    identifier_t nextIdentifier, PipelineId::Underlying pipelineIdCounter, OriginId::Underlying originIdCounter)
    : nextIdentifier(nextIdentifier), pipelineIdCounter(pipelineIdCounter), originIdCounter(originIdCounter)
{
}

TestingHarness::TestingHarness(size_t numberOfThreads, size_t numberOfBuffers)
    : bm(Memory::BufferManager::create(DEFAULT_BUFFER_SIZE, numberOfBuffers)), numberOfThreads(numberOfThreads)
{
}

TestingHarness::TestingHarness() : TestingHarness(NUMBER_OF_THREADS, NUMBER_OF_BUFFERS_PER_SOURCE)
{
}

QueryPlanBuilder TestingHarness::buildNewQuery() const
{
    return QueryPlanBuilder{lastIdentifier, lastPipelineIdCounter, lastOriginIdCounter};
}

std::unique_ptr<Runtime::ExecutableQueryPlan> TestingHarness::addNewQuery(QueryPlanBuilder&& builder)
{
    const auto queryId = QueryId(queryIdCounter++);
    lastIdentifier = builder.nextIdentifier;
    lastOriginIdCounter = builder.originIdCounter;
    lastPipelineIdCounter = builder.pipelineIdCounter;

    auto [plan, pSourceIds, pPipelineIds, pSourceCtrls, pSinkCtrls, pPipelineCtrls, pStages] = std::move(builder).build(queryId, bm);
    sourceIds.insert(pSourceIds.begin(), pSourceIds.end());
    pipelineIds.insert(pPipelineIds.begin(), pPipelineIds.end());
    sourceControls.insert(pSourceCtrls.begin(), pSourceCtrls.end());
    sinkControls.insert(pSinkCtrls.begin(), pSinkCtrls.end());
    pipelineControls.insert(pPipelineCtrls.begin(), pPipelineCtrls.end());
    stages.insert(pStages.begin(), pStages.end());
    return std::move(plan);
}

void TestingHarness::expectQueryStatusEvents(QueryId id, std::initializer_list<Runtime::Execution::QueryStatus> states)
{
    for (auto state : states)
    {
        switch (state)
        {
            case Runtime::Execution::QueryStatus::Registered:
                EXPECT_CALL(*status, logQueryStatusChange(id, Runtime::Execution::QueryStatus::Registered, ::testing::_)).Times(1);
                break;
            case Runtime::Execution::QueryStatus::Started:
                EXPECT_CALL(*status, logQueryStatusChange(id, Runtime::Execution::QueryStatus::Started, ::testing::_))
                    .Times(1)
                    .WillOnce(::testing::Invoke([](auto, auto, auto) { return true; }));
                break;
            case Runtime::Execution::QueryStatus::Running:
                queryRunning.emplace(id, std::make_unique<std::promise<void>>());
                EXPECT_CALL(*status, logQueryStatusChange(id, Runtime::Execution::QueryStatus::Running, ::testing::_))
                    .Times(1)
                    .WillOnce(::testing::Invoke(
                        [this](auto id, auto, auto)
                        {
                            queryRunning.at(id)->set_value();
                            return true;
                        }));
                break;
            case Runtime::Execution::QueryStatus::Stopped:
                ASSERT_TRUE(queryTermination.try_emplace(id, std::make_unique<std::promise<void>>()).second)
                    << "Registered multiple query terminations";
                EXPECT_CALL(*status, logQueryStatusChange(id, Runtime::Execution::QueryStatus::Stopped, ::testing::_))
                    .Times(1)
                    .WillOnce(::testing::Invoke(
                        [this](auto id, auto, auto)
                        {
                            queryTermination.at(id)->set_value();
                            return true;
                        }));
                break;
            case Runtime::Execution::QueryStatus::Failed:
                ASSERT_TRUE(queryTermination.try_emplace(id, std::make_unique<std::promise<void>>()).second)
                    << "Registered multiple query terminations";
                EXPECT_CALL(*status, logQueryFailure(id, ::testing::_, ::testing::_))
                    .Times(1)
                    .WillOnce(::testing::Invoke(
                        [this](const auto& id, const auto&, auto)
                        {
                            queryTermination.at(id)->set_value();
                            return true;
                        }));
                break;
        }
    }
}

void TestingHarness::expectSourceTermination(QueryId queryId, QueryPlanBuilder::identifier_t source, Runtime::QueryTerminationType type)
{
    EXPECT_CALL(*status, logSourceTermination(queryId, sourceIds.at(source), type, ::testing::_)).WillOnce(::testing::Return(true));
}

void TestingHarness::start()
{
    for (const auto& queryTermination : queryTermination)
    {
        queryTerminationFutures[queryTermination.first] = queryTermination.second->get_future().share();
    }
    for (const auto& queryRunning : queryRunning)
    {
        queryRunningFutures[queryRunning.first] = queryRunning.second->get_future().share();
    }
    Runtime::QueryEngineConfiguration configuration{};
    configuration.numberOfWorkerThreads.setValue(numberOfThreads);
    qm = std::make_unique<NES::Runtime::QueryEngine>(configuration, this->statListener, this->status, this->bm);
}

void TestingHarness::startQuery(std::unique_ptr<Runtime::ExecutableQueryPlan> query) const
{
    qm->start(std::move(query));
}

void TestingHarness::stopQuery(QueryId id) const
{
    qm->stop(id);
}

void TestingHarness::stop()
{
    qm.reset();
}

testing::AssertionResult TestingHarness::waitForQepTermination(QueryId id, std::chrono::milliseconds timeout) const
{
    return waitForFuture(queryTerminationFutures.at(id), timeout);
}

testing::AssertionResult TestingHarness::waitForQepRunning(QueryId id, std::chrono::milliseconds timeout)
{
    return waitForFuture(queryRunningFutures.at(id), timeout);
}
}
