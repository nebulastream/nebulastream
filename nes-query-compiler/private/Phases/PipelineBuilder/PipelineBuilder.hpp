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
#include <Phases/PipelineBuilder/AbstractStateMachine.hpp>
#include <Phases/PipelineBuilder/PipelineBuilderState.hpp>
#include <Phases/PipelineBuilder/PipelineBuilderTransitions.hpp>
#include <PhysicalPlan.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES
{

/// NOTE: think twice before changing this file to extend the builders' functionality.
/// Most extensions should be possible by extending @link PipeliningBuildingDefintion.cpp
class PipelineBuilder final : public AbstractStateMachine<State, Event, BuilderContext>
{
public:
    PipelineBuilder() : AbstractStateMachine(transitions, State::Start, State::Invalid) { }

    std::shared_ptr<PipelinedQueryPlan> build(const PhysicalPlan& plan)
    {
        try
        {
            /// Init context
            BuilderContext ctx{
                plan,
                plan.getRootOperators().front(),
                nullptr,
                nullptr,
                {},
                std::make_shared<PipelinedQueryPlan>(plan.getQueryId(), plan.getExecutionMode()),
                plan.getOperatorBufferSize(),
                {}};

            /// Add for each root operator a frame
            for (auto it = plan.getRootOperators().rbegin(); it != plan.getRootOperators().rend(); ++it)
            {
                ctx.contextStack.push_back(Frame{*it, nullptr, nullptr, 0});
            }

            /// while there are frames to process
            while (!ctx.contextStack.empty())
            {
                /// Load frame into current context
                Frame& frame = ctx.contextStack.back();
                ctx.currentOp = frame.op;
                ctx.prevOp = frame.prev;
                ctx.currentPipeline = frame.pipeline;

                /// 1) First visit: process the operator exactly once
                if (frame.nextChildIdx == 0)
                {
                    const Event event = deriveEvent(ctx, getState());
                    this->step(event, ctx);
                }

                /// 2) If there are still children to visit, descend into the next one
                if (frame.nextChildIdx < ctx.currentOp->getChildren().size())
                {
                    this->step(Event::DescendChild, ctx);
                    continue;
                }

                /// 3) No children left: finish this node (bubble up or finish root)
                if (frame.prev)
                {
                    this->step(Event::ChildDone, ctx);
                    continue;
                }

                /// 4) Root finished: close FSM and drop the root frame
                this->step(Event::EncounterEnd, ctx);
                ctx.contextStack.pop_back();

                /// If we just finished processing a root operator's tree,
                /// but there are more roots on the stack, we must reset the FSM.
                if (!ctx.contextStack.empty())
                {
                    this->reset();
                }
            }
            INVARIANT(getState() == State::Success, "did not reach success state after all operators were processed");

            return ctx.outPlan;
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Failed to build PipelinedQueryPlan: . Error: {}.", e.what());
            throw;
        }
    }

private:
    State step(Event event, BuilderContext& ctx)
    {
        State currentState = this->getState();
        State newState = AbstractStateMachine::step(event, ctx);
        if (newState == State::Invalid)
        {
            handleInvalidState(currentState, event, ctx);
        }

        return newState;
    }

    void handleInvalidState(State fromState, Event event, const BuilderContext& ctx)
    {
        NES_ERROR("PipelineBuilder: Invalid state transition detected");
        NES_ERROR("  From State: {}", magic_enum::enum_name(fromState));
        NES_ERROR("  Event: {}", magic_enum::enum_name(event));
        NES_ERROR("  Current Operator: {}", ctx.currentOp ? ctx.currentOp->explain(ExplainVerbosity::Short) : "null");

        NES_ERROR("  History: {}", getHistoryAsString());


        throw InvalidQuerySyntax(
            "Invalid state transition in PipelineBuilder State Machine. Trace log: {}From: {}, Event: {}",
            getHistoryAsString(),
            magic_enum::enum_name(fromState),
            magic_enum::enum_name(event));
    }

    /// derives for the current operator the event
    static Event deriveEvent(const BuilderContext& ctx, State curState)
    {
        if (ctx.currentOp->getPhysicalOperator().tryGet<SinkPhysicalOperator>())
        {
            return Event::EncounterSink;
        }

        if (ctx.currentOp->getPhysicalOperator().tryGet<SourcePhysicalOperator>())
        {
            return Event::EncounterSource;
        }

        /// Operator-kind events
        switch (ctx.currentOp->getPipelineLocation())
        {
            case PhysicalOperatorWrapper::PipelineLocation::SCAN:
                return Event::EncounterCustomScan;
            case PhysicalOperatorWrapper::PipelineLocation::EMIT:
                return Event::EncounterCustomEmit;
            case PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE:
                break;
        }

        /// E6 – revisit
        if (ctx.op2Pipeline.count(ctx.currentOp->getPhysicalOperator().getId()))
        {
            return Event::EncounterKnownOperator;
        }

        /// E7 – force-new requested by state machine
        if (curState == State::ForcePipelineBreak)
        {
            return Event::ForceNewFlag;
        }

        /// E5 – need default emit (leaf & *not* already an EMIT)
        if (ctx.currentOp->getChildren().empty() && ctx.currentOp->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT
            && curState == State::BuildingPipeline)
        {
            return Event::NeedDefaultEmit;
        }

        /// Default
        return Event::EncounterFusibleOperator;
    }
};

}
