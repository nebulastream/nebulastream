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

#include <QueryManagerTestingInfrastructure.hpp>

namespace NES::Testing
{

std::vector<std::byte> identifiableData(size_t identifier)
{
    std::vector<std::byte> data(8192);
    size_t stepSize = sizeof(identifier) / sizeof(std::byte);
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

    size_t stepSize = sizeof(identifier) / sizeof(std::byte);
    bool allMatch = true;
    for (size_t index = 0; index < buffer.getBufferSize() / stepSize; index += stepSize)
    {
        allMatch |= *std::bit_cast<size_t*>(&buffer.getBuffer<std::byte>()[stepSize]) == identifier;
    }

    return allMatch;
}

testing::AssertionResult TestSinkController::waitForNumberOfReceivedBuffers(size_t numberOfExpectedBuffers)
{
    auto buffers = receivedBuffers.lock();
    if (buffers->size() >= numberOfExpectedBuffers)
    {
        return testing::AssertionSuccess();
    }

    auto check = receivedBufferTrigger.wait_for(
        buffers.as_lock(), std::chrono::milliseconds(100), [&]() { return buffers->size() >= numberOfExpectedBuffers; });

    if (check)
    {
        return testing::AssertionSuccess();
    }
    else
    {
        return testing::AssertionFailure() << "The expected number of tuple buffers were not received after 100ms";
    }
}

void TestSinkController::insertBuffer(Memory::TupleBuffer&& buffer)
{
    receivedBuffers.lock()->push_back(std::move(buffer));
    receivedBufferTrigger.notify_one();
}

std::vector<Memory::TupleBuffer> TestSinkController::takeBuffers()
{
    return receivedBuffers.exchange({});
}

std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestSinkController>>
createSinkPipeline(std::shared_ptr<Memory::AbstractBufferProvider> bm)
{
    auto sinkController = std::make_shared<TestSinkController>();
    auto stage = std::make_unique<TestSink>(std::move(bm), sinkController);
    auto pipeline = std::make_shared<Runtime::Execution::ExecutablePipeline>(
        std::move(stage), std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>{});
    return {pipeline, sinkController};
}
std::tuple<std::shared_ptr<Runtime::Execution::ExecutablePipeline>, std::shared_ptr<TestPipelineController>>
createPipeline(const std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>& successors)
{
    auto pipelineCtrl = std::make_shared<TestPipelineController>();
    auto stage = std::make_unique<TestPipeline>(pipelineCtrl);
    auto pipeline = std::make_shared<Runtime::Execution::ExecutablePipeline>(std::move(stage), successors);
    return {pipeline, pipelineCtrl};
}
QueryPlanBuilder::identifier_t QueryPlanBuilder::addPipeline(std::vector<identifier_t> predecssors)
{
    auto identifier = nextIdentifier++;
    for (auto pred : predecssors)
    {
        assert(!std::holds_alternative<SinkDescriptor>(objects[pred]) && "Sink Descriptor cannot be a predecessor");
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
QueryPlanBuilder::identifier_t QueryPlanBuilder::addSink(std::vector<identifier_t> predecessors)
{
    auto identifier = nextIdentifier++;
    for (auto pred : predecessors)
    {
        assert(!std::holds_alternative<SinkDescriptor>(objects[pred]) && "Sink Descriptor cannot be a predecessor");
        forwardRelations[pred].push_back(identifier);
        backwardRelations[identifier].push_back(pred);
    }

    objects[identifier] = SinkDescriptor{};
    return identifier;
}
QueryPlanBuilder::TestPlanCtrl QueryPlanBuilder::build(std::shared_ptr<Memory::BufferManager> bm) &&
{
    auto isSource = std::ranges::views::filter([](const std::pair<identifier_t, ObjectDescriptor>& kv)
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
    std::unordered_map<identifier_t, std::shared_ptr<TestPipelineOperatorHandlerController>> pipelineOperatorHandlerCtrls;
    std::unordered_map<identifier_t, std::shared_ptr<Runtime::Execution::ExecutablePipeline>> cache{};
    std::function<std::shared_ptr<Runtime::Execution::ExecutablePipeline>(identifier_t)> get_or_create_pipeline
        = [&](identifier_t identifier)
    {
        if (auto it = cache.find(identifier); it != cache.end())
        {
            return it->second;
        }

        auto result = std::visit(
            overloaded{
                [](SourceDescriptor) -> std::shared_ptr<Runtime::Execution::ExecutablePipeline>
                { assert(false && "Source cannot be a successor"); },
                [&](SinkDescriptor) -> std::shared_ptr<Runtime::Execution::ExecutablePipeline>
                {
                    auto [sink, ctrl] = createSinkPipeline(bm);
                    pipelines.push_back(sink);
                    stages[identifier] = sink->stage.get();
                    sinkCtrls[identifier] = ctrl;
                    return pipelines.back();
                },
                [&](PipelineDescriptor descriptor) -> std::shared_ptr<Runtime::Execution::ExecutablePipeline>
                {
                    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> successors;
                    std::ranges::transform(forwardRelations.at(identifier), std::back_inserter(successors), get_or_create_pipeline);
                    auto [pipeline, pipelineCtrl] = createPipeline(std::move(successors));
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
        std::ranges::transform(forwardRelations.at(source.first), std::back_inserter(successors), get_or_create_pipeline);
        auto [s, ctrl] = Sources::getTestSource(std::get<SourceDescriptor>(source.second).sourceId, bm);
        sourceIds.emplace(source.first, s->getSourceId());
        sources.emplace_back(std::move(s), std::move(successors));
        sourceCtrls[source.first] = ctrl;
    }

    return {
        std::make_unique<Runtime::InstantiatedQueryPlan>(std::move(pipelines), std::move(sources)),
        sourceIds,
        pipelineIds,
        sourceCtrls,
        sinkCtrls,
        pipelineCtrls,
        pipelineOperatorHandlerCtrls,
        stages};
}

QueryPlanBuilder TestingHarness::buildNewQuery()
{
    return QueryPlanBuilder{lastIdentifier, lastPipelineIdCounter, lastOriginIdCounter};
}

std::unique_ptr<Runtime::InstantiatedQueryPlan> TestingHarness::addNewQuery(QueryPlanBuilder&& builder)
{
    lastIdentifier = builder.nextIdentifier;
    lastOriginIdCounter = builder.originIdCounter;
    lastPipelineIdCounter = builder.pipelineIdCounter;

    auto [plan, pSourceIds, pPipelineIds, pSourceCtrls, pSinkCtrls, pPipelineCtrls, pOperatorHandlerCtrls, pStages]
        = std::move(builder).build(bm);
    sourceIds.insert(pSourceIds.begin(), pSourceIds.end());
    pipelineIds.insert(pPipelineIds.begin(), pPipelineIds.end());
    sourceControls.insert(pSourceCtrls.begin(), pSourceCtrls.end());
    sinkControls.insert(pSinkCtrls.begin(), pSinkCtrls.end());
    pipelineControls.insert(pPipelineCtrls.begin(), pPipelineCtrls.end());
    operatorHandlerControls.insert(pOperatorHandlerCtrls.begin(), pOperatorHandlerCtrls.end());
    stages.insert(pStages.begin(), pStages.end());
    return std::move(plan);
}

void TestingHarness::expectQueryStatusEvents(QueryId id, std::initializer_list<Runtime::Execution::QueryStatus> states)
{
    queryRunning.emplace(id, std::make_unique<std::promise<void>>());
    queryTermination.emplace(id, std::make_unique<std::promise<void>>());
    for (auto state : states)
    {
        if (state == Runtime::Execution::QueryStatus::Failed)
        {
            EXPECT_CALL(*status, logQueryFailure(id, ::testing::_))
                .Times(1)
                .WillOnce(::testing::Invoke(
                    [this](const auto& id, const auto&)
                    {
                        queryTermination.at(id)->set_value();
                        return true;
                    }));
        }
        else
        {
            EXPECT_CALL(*status, logQueryStatusChange(::testing::Eq(id), ::testing::Eq(state)))
                .Times(1)
                .WillOnce(::testing::Invoke(
                    [this](const auto& id, const auto& state)
                    {
                        if (state == Runtime::Execution::QueryStatus::Stopped)
                        {
                            queryTermination.at(id)->set_value();
                        }
                        else if (state == Runtime::Execution::QueryStatus::Running)
                        {
                            queryRunning.at(id)->set_value();
                        }

                        return true;
                    }));
        }
    }
}

void TestingHarness::expectSourceTermination(QueryId queryId, QueryPlanBuilder::identifier_t source, Runtime::QueryTerminationType type)
{
    EXPECT_CALL(*status, logSourceTermination(queryId, sourceIds.at(source), type)).WillOnce(::testing::Return(true));
}

void TestingHarness::start()
{
    for (const auto& query_termination : queryTermination)
    {
        queryTerminationFutures[query_termination.first] = query_termination.second->get_future().share();
    }
    for (const auto& queryRunning : queryRunning)
    {
        queryRunningFutures[queryRunning.first] = queryRunning.second->get_future().share();
    }
    qm = std::make_unique<NES::Runtime::QueryEngine>(numberOfThreads, this->status, this->bm);
}
}
