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

#pragma once

#include <memory>
#include <EmitOperatorHandler.hpp>
#include <PhysicalPlan.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES
{

enum class State : uint8_t
{
    Start,
    BuildingPipeline,
    PendingEmit,
    AfterEmit,
    RevisitOperator,
    SourceCreated,
    SinkCreated,
    ForcePipelineBreak,
    Success,
    Invalid,
};

enum class Event : uint8_t
{
    DescendChild,
    ChildDone,
    RootComplete,
    EncounterSource,
    EncounterCustomScan,
    EncounterCustomEmit,
    EncounterSink,
    EncounterFusibleOperator,
    NeedDefaultEmit,
    EncounterKnownOperator,
    ForceNewFlag,
    EncounterEnd,
};

struct Frame
{
    std::shared_ptr<PhysicalOperatorWrapper> op;
    std::shared_ptr<PhysicalOperatorWrapper> prev;
    std::shared_ptr<Pipeline> pipeline;
    std::size_t nextChildIdx{0};
};

struct BuilderContext
{
    using OperatorPipelineMap = std::unordered_map<OperatorId, std::shared_ptr<Pipeline>>;

    /// Immutable session data
    const PhysicalPlan& physicalPlan;

    /// Mutable traversal state (one copy, plus stack frames)
    std::shared_ptr<PhysicalOperatorWrapper> currentOp;
    std::shared_ptr<PhysicalOperatorWrapper> prevOp; /// keeps the last operator in the current pipeline
    std::shared_ptr<Pipeline> currentPipeline;

    /// Global build artefacts
    OperatorPipelineMap op2Pipeline;
    std::shared_ptr<PipelinedQueryPlan> outPlan;
    uint64_t bufferSize;

    /// Explicit replacement for recursion stack
    std::vector<Frame> contextStack;
};

}
