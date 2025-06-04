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
    //  Start‑up (root operators)
    {State::Start, Event::EncounterSource, State::SourceCreated, {appendSource}},
    // {State::Start,              Event::EncounterCustomScan,        State::BuildingPipeline,   {createPipeline, prependDefaultScan, registerHandler}},
    // {State::Start,              Event::EncounterSink,              State::SinkCreated,        {createPipeline, registerHandler}},
    // {State::Start,              Event::EncounterCustomEmit,        State::AfterEmit,          {createPipeline, registerHandler, appendOperator}},
    // {State::Start,              Event::EncounterFusibleOperator,   State::BuildingPipeline,   {createPipeline, registerHandler, appendOperator}},
    // {State::Start,              Event::EncounterKnownOperator,     State::RevisitOperator,    {addSuccessor}},

    //  SourceCreated
    {State::SourceCreated, Event::EncounterSink, State::Success, {appendSink}},
    {State::SourceCreated, Event::DescendChild, State::SourceCreated, {pushContext}},

    //  BuildingPipeline  (fusible operators accumulate here)
    {State::BuildingPipeline, Event::EncounterFusibleOperator, State::BuildingPipeline, {appendOperator, registerHandler}},
    //    {State::BuildingPipeline,   Event::EncounterCustomScan,        State::BuildingPipeline,   {appendDefaultEmitIfNeeded, createPipeline, prependDefaultScan, registerHandler, addSuccessor}},
    {State::BuildingPipeline, Event::EncounterCustomEmit, State::AfterEmit, {appendOperator, registerHandler}},
    //    {State::BuildingPipeline,   Event::EncounterSink,              State::SinkCreated,        {appendDefaultEmitIfNeeded, createPipeline, registerHandler, addSuccessor}},
    {State::BuildingPipeline, Event::NeedDefaultEmit, State::PendingEmit, {appendDefaultEmit}},
    //    {State::BuildingPipeline,   Event::EncounterKnownOperator,     State::RevisitOperator,    {appendDefaultEmitIfNeeded, addSuccessor}},
    //    {State::BuildingPipeline,   Event::ForceNewFlag,               State::ForcePipelineBreak, {appendDefaultEmitIfNeeded, createPipeline, prependDefaultScan, addSuccessor}},
    {State::BuildingPipeline, Event::ChildDone, State::BuildingPipeline, {popContext}},
    {State::BuildingPipeline, Event::RootComplete, State::Start, {popContext}},
    {State::BuildingPipeline, Event::DescendChild, State::BuildingPipeline, {pushContext}},

    //  PendingEmit  (default emit has just been inserted, no operator yet)
    {State::PendingEmit,
     Event::EncounterFusibleOperator,
     State::BuildingPipeline,
     {createPipeline, prependDefaultScan, registerHandler, addSuccessor}},
    {State::PendingEmit,
     Event::EncounterCustomScan,
     State::BuildingPipeline,
     {createPipeline, prependDefaultScan, registerHandler, addSuccessor}},
    {State::PendingEmit, Event::EncounterCustomEmit, State::AfterEmit, {createPipeline, registerHandler, addSuccessor}},
    {State::PendingEmit, Event::EncounterSink, State::SinkCreated, {createPipeline, registerHandler, addSuccessor}},
    {State::PendingEmit, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::PendingEmit, Event::ForceNewFlag, State::ForcePipelineBreak, {createPipeline, prependDefaultScan, addSuccessor}},

    //  AfterEmit  (pipeline is closed; next op starts new pipeline)
    {State::AfterEmit,
     Event::EncounterFusibleOperator,
     State::BuildingPipeline,
     {createPipeline, prependDefaultScan, registerHandler, addSuccessor}},
    {State::AfterEmit,
     Event::EncounterCustomScan,
     State::BuildingPipeline,
     {createPipeline, prependDefaultScan, registerHandler, addSuccessor}},
    {State::AfterEmit, Event::EncounterCustomEmit, State::AfterEmit, {createPipeline, registerHandler, appendOperator, addSuccessor}},
    {State::AfterEmit, Event::EncounterSink, State::SinkCreated, {createPipeline, registerHandler, addSuccessor}},
    {State::AfterEmit, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::AfterEmit, Event::ForceNewFlag, State::ForcePipelineBreak, {createPipeline, prependDefaultScan, addSuccessor}},

    //  RevisitOperator  (operator already has a pipeline)
    {State::RevisitOperator, Event::DescendChild, State::BuildingPipeline, {pushContext}},
    {State::RevisitOperator, Event::ChildDone, State::RevisitOperator, {popContext}},
    {State::RevisitOperator, Event::RootComplete, State::Start, {popContext}},

    //  SinkCreated  (a sink operator forms its own pipeline)
    {State::SinkCreated, Event::DescendChild, State::BuildingPipeline, {pushContext}},
    {State::SinkCreated, Event::ChildDone, State::SinkCreated, {popContext}},
    {State::SinkCreated, Event::EncounterFusibleOperator, State::BuildingPipeline, {appendOperator, registerHandler}},
    {State::SinkCreated, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::SinkCreated, Event::EncounterEnd, State::Success, {}},


    //  ForcePipelineBreak  (policy‑induced)
    {State::ForcePipelineBreak, Event::EncounterFusibleOperator, State::BuildingPipeline, {appendOperator, registerHandler}},
    {State::ForcePipelineBreak, Event::EncounterCustomEmit, State::AfterEmit, {appendOperator, registerHandler}},
    {State::ForcePipelineBreak, Event::EncounterSink, State::SinkCreated, {createPipeline, registerHandler}},
    {State::ForcePipelineBreak, Event::EncounterKnownOperator, State::RevisitOperator, {addSuccessor}},
    {State::ForcePipelineBreak, Event::DescendChild, State::BuildingPipeline, {pushContext}},

};

}
