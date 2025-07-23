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
/// TODO max steps 10.000


/// NOTE: think twice before changing this file to extend the builders' functionality.
/// Most extensions should be possible by extending @link PipeliningBuildingDefintion.cpp
class PipelineBuilder final : public AbstractStateMachine<State, Event, BuilderContext>
{
public:
    PipelineBuilder() : AbstractStateMachine(transitions, State::Start, State::Invalid) { }

    std::shared_ptr<PipelinedQueryPlan> build(const PhysicalPlan& plan)
    {
        PipelineBuilder fsm{};

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
            /// load frame into current context
            Frame& frame = ctx.contextStack.back();
            ctx.currentOp = frame.op;
            ctx.prevOp = frame.prev;
            ctx.currentPipeline = frame.pipeline;

            /// 1. process current operator
            const Event event = deriveEvent(ctx, this->getState());
            // DEBUG: Print derived event
            std::cout << "[DEBUG] Derived event: " << static_cast<int>(event) << std::endl;
            const auto state = fsm.step(event, ctx);
            // DEBUG: Print new state after step
            std::cout << "[DEBUG] New state after step: " << static_cast<int>(state) << std::endl;

            // DEBUG: Print current operator ID and state
            std::cout << "[DEBUG] Processing operator ID: " << ctx.currentOp->explain(ExplainVerbosity::Short) << std::endl;
            /// Add children to context stack
            if (frame.nextChildIdx < ctx.currentOp->getChildren().size())
            {
                // auto child = ctx.currentOp->getChildren()[frame.nextChildIdx++];
                // ctx.contextStack.push_back(Frame{child, ctx.currentOp, ctx.currentPipeline, 0});
            }
            else
            {
                /// No children left: remove from stack
                ctx.contextStack.pop_back();
            }

            /// 2. descend into first child if any
            if (not ctx.currentOp->getChildren().empty() and (ctx.contextStack.empty() or ctx.contextStack.back().nextChildIdx == 0))
            {
                std::cout << "[DEBUG] DescendChild event triggered for operator ID:" << ctx.currentOp->explain(ExplainVerbosity::Short) << std::endl;
                fsm.step(Event::DescendChild, ctx);
                continue;
            }

            /// 3. if we just finished a child, bubble up with ChildDone
            while (not ctx.contextStack.empty()
                   and ctx.contextStack.back().nextChildIdx >= ctx.contextStack.back().prev->getChildren().size())
            {
                std::cout << "[DEBUG] ChildDone event triggered for operator ID:"  << ctx.currentOp->explain(ExplainVerbosity::Short) << std::endl;
                fsm.step(Event::ChildDone, ctx);
            }

            /// 4. root finished --> exit
            if (ctx.contextStack.empty())
            {
                INVARIANT(state == State::Success, "did not reach success state after all operators where processed");
                break;
            }
        }

        /// TODO check if we reached the final state
        return ctx.outPlan;
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


        throw InvalidQuerySyntax("Invalid state transition in PipelineBuilder State Machine. Trace log: {}From: {}, Event: {}", getHistoryAsString(), magic_enum::enum_name(fromState), magic_enum::enum_name(event));
        // TODO
    }

    /// derives for the current operator the event
    static Event deriveEvent(const BuilderContext& ctx, State curState)
    {
        /// E6 – revisit
        if (ctx.op2Pipeline.count(ctx.currentOp->getPhysicalOperator().getId()))
        {
            return Event::EncounterKnownOperator;
        }

        /// E5 – need default emit (leaf & *not* already an EMIT)
        if (ctx.currentOp->getChildren().empty() && ctx.currentOp->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT
            && curState == State::BuildingPipeline)
        {
            return Event::NeedDefaultEmit;
        }

        /// E7 – force-new requested by state machine
        if (curState == State::ForcePipelineBreak)
        {
            return Event::ForceNewFlag;
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

        if (ctx.currentOp->getPhysicalOperator().tryGet<SinkPhysicalOperator>())
        {
            return Event::EncounterSink;
        }

        if (ctx.currentOp->getPhysicalOperator().tryGet<SourcePhysicalOperator>())
        {
            return Event::EncounterSource;
        }

        /// Default
        return Event::EncounterFusibleOperator;
    }
};

}
