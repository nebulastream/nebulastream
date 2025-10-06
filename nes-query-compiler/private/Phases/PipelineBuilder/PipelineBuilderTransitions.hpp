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

#include <Phases/PipelineBuilder/PipelineBuilderActions.hpp>
#include <Phases/PipelineBuilder/PipelineBuilderState.hpp>

namespace NES
{

using T = Transition<State, Event, BuilderContext>;

static inline const std::vector<T> transitions = {
    ///  Start‑up (root operators)
    {State::Start, Event::EncounterSource, State::SourceContext, {appendSource}},

    /// SourceContext
    {State::SourceContext, Event::DescendChild, State::SourceCreated, {pushContext}},

    ///  SourceCreated
    {State::SourceCreated, Event::EncounterFusibleOperator, State::BuildingPipeline, {createPipeline, prependDefaultScan}},
    {State::SourceCreated, Event::EncounterSink, State::SinkCreated, {appendSink}},
    {State::SourceCreated, Event::EncounterCustomScan, State::BuildingPipeline, {createPipeline, registerHandler}},

    /// BuildingPipeline  (fusible operators accumulate here)
    {State::BuildingPipeline, Event::EncounterFusibleOperator, State::BuildingPipeline, {appendOperator, registerHandler}},
    {State::BuildingPipeline,
     Event::EncounterCustomScan,
     State::BuildingPipeline,
     {appendDefaultEmitIfNeeded, createPipeline, registerHandler}},
    {State::BuildingPipeline, Event::EncounterCustomEmit, State::AfterEmit, {appendOperator, registerHandler}},
    {State::BuildingPipeline, Event::EncounterSink, State::SinkCreated, {appendDefaultEmit, appendSink}},
    {State::BuildingPipeline, Event::NeedDefaultEmit, State::PendingEmit, {appendDefaultEmit}},
    {State::BuildingPipeline, Event::EncounterKnownOperator, State::RevisitOperator, {appendDefaultEmitIfNeeded, addSuccessor}},
    {State::BuildingPipeline,
     Event::ForceNewFlag,
     State::ForcePipelineBreak,
     {appendDefaultEmitIfNeeded, createPipeline, prependDefaultScan}},
    {State::BuildingPipeline, Event::ChildDone, State::BuildingPipeline, {popContext}},
    {State::BuildingPipeline, Event::RootComplete, State::Start, {popContext}},
    {State::BuildingPipeline, Event::DescendChild, State::BuildingPipeline, {pushContext}},
    {State::BuildingPipeline, Event::EncounterEnd, State::Success, {}},

    ///  PendingEmit  (default emit has just been inserted, no operator yet)
    {State::PendingEmit, Event::EncounterFusibleOperator, State::BuildingPipeline, {createPipeline, prependDefaultScan, registerHandler}},
    {State::PendingEmit, Event::EncounterCustomScan, State::BuildingPipeline, {createPipeline, prependDefaultScan, registerHandler}},
    {State::PendingEmit, Event::EncounterCustomEmit, State::AfterEmit, {createPipeline, registerHandler}},
    {State::PendingEmit, Event::EncounterSink, State::SinkCreated, {appendSink}},
    {State::PendingEmit, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::PendingEmit, Event::ForceNewFlag, State::ForcePipelineBreak, {createPipeline, prependDefaultScan}},
    {State::PendingEmit, Event::ChildDone, State::AfterEmit, {popContext}},
    {State::PendingEmit, Event::RootComplete, State::Start, {popContext}},

    /// AfterEmit  (pipeline is closed; next op starts new pipeline)
    {State::AfterEmit, Event::EncounterFusibleOperator, State::BuildingPipeline, {createPipeline, prependDefaultScan, registerHandler}},
    {State::AfterEmit, Event::EncounterCustomScan, State::BuildingPipeline, {createPipeline, registerHandler}},
    {State::AfterEmit, Event::EncounterCustomEmit, State::AfterEmit, {createPipeline, registerHandler, appendOperator}},
    {State::AfterEmit, Event::EncounterSink, State::SinkCreated, {appendSink}},
    {State::AfterEmit, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::AfterEmit, Event::ForceNewFlag, State::ForcePipelineBreak, {createPipeline, prependDefaultScan}},
    {State::AfterEmit, Event::DescendChild, State::BuildingPipeline, {pushContext}},
    {State::AfterEmit, Event::ChildDone, State::AfterEmit, {popContext}},

    ///  RevisitOperator  (operator already has a pipeline)
    {State::RevisitOperator, Event::DescendChild, State::BuildingPipeline, {pushContext}},
    {State::RevisitOperator, Event::ChildDone, State::BuildingPipeline, {popContext}},
    {State::RevisitOperator, Event::RootComplete, State::Start, {popContext}},

    ///  SinkCreated  (a sink operator forms its own pipeline)
    {State::SinkCreated, Event::DescendChild, State::BuildingPipeline, {pushContext}},
    {State::SinkCreated, Event::ChildDone, State::BuildingPipeline, {popContext}},
    {State::SinkCreated, Event::EncounterFusibleOperator, State::BuildingPipeline, {appendOperator, registerHandler}},
    {State::SinkCreated, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::SinkCreated, Event::EncounterEnd, State::Success, {}},

    ///  ForcePipelineBreak  (policy‑induced)
    {State::ForcePipelineBreak, Event::EncounterFusibleOperator, State::BuildingPipeline, {appendOperator, registerHandler}},
    {State::ForcePipelineBreak, Event::EncounterCustomEmit, State::AfterEmit, {appendOperator, registerHandler}},
    {State::ForcePipelineBreak, Event::EncounterSink, State::SinkCreated, {appendSink}},
    {State::ForcePipelineBreak, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::ForcePipelineBreak, Event::DescendChild, State::BuildingPipeline, {pushContext}},
};

}
