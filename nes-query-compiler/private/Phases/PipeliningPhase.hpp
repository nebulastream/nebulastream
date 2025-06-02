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
#include <EmitPhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{


struct Action
{
    bool prependScan       = false;
    bool appendEmit        = false;
    bool startNewPipeline  = false;
    bool appendOperator    = false;
};

/// During this step we create a PipelinedQueryPlan out of the QueryPlan obj
std::shared_ptr<PipelinedQueryPlan> apply(const PhysicalPlan& queryPlan);

/// Pipeline builder finite state machine. We apply the idiomatic state, event, action model.
///
/// Transition-Table: (currentState, opType, policy) -> (nextState, action)
///
/// 1. (Idle,       Source)      -> (InPipeline,  startNewPipeline on root)
/// 2. (InPipeline, Scan)        -> (InPipeline,  prependScan, startNewPipeline)
/// 3. (InPipeline, emit)        -> (AfterEmit,   appendOperator)
/// 4. (AfterEmit,  any)         -> (InPipeline,  startNewPipeline, prependScan)
/// 5. (InPipeline, Sink)        -> (AfterSink,   appendEmit, startNewPipeline)
/// 6. (InPipeline, fusible)     -> (InPipeline,  appendOperator)
/// 7. (InPipeline, fusible)     -> (AfterEmit,   appendEmit, startNewPipeline)
class PipelineBuilderFSM
{
    using OperatorPipelineMap = std::unordered_map<OperatorId, std::shared_ptr<Pipeline>>;
    using Operator = const std::shared_ptr<PhysicalOperatorWrapper>&;

    enum class State : uint8_t
    {
        Idle,           /// haven't started a pipelining yet
        InPipeline,     /// building within an existing pipeline
        AfterEmit,      /// just closed a pipeline via emit
        AfterSink,      /// just closed a pipeline via a sink
        Invalid,        /// invalid state,
    };

    enum class Event : uint8_t
    {
        Source,
        FusibleOperator,
        CustomScan,
        CustomEmit,
        Sink,
    };

    using ActionHandler = std::function<
        void(Action action,
       const std::shared_ptr<PhysicalOperatorWrapper>& op,
       std::shared_ptr<Pipeline> currentPipeline,
       OperatorPipelineMap pipelineMap)>;

    struct Transition
    {
        State          from;
        Event          onEvent;
        State          toState;
        Action         action;
        ActionHandler  handler;
    };

    /// Helper function to add a default scan operator
    /// This is used only when the wrapped operator does not already provide a scan
    /// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
    /// Do not add further parameters here that should be part of the QueryOptimizerConfiguration.
    void addDefaultScan(const std::shared_ptr<Pipeline>& pipeline, const PhysicalOperatorWrapper& wrappedOp, uint64_t configuredBufferSize)
    {
        PRECONDITION(pipeline->isOperatorPipeline(), "Only add scan physical operator to operator pipelines");
        auto schema = wrappedOp.getInputSchema();
        INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

        auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(configuredBufferSize, schema.value());
        const auto memoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
        /// Prepend the default scan operator.
        pipeline->prependOperator(ScanPhysicalOperator(memoryProvider, schema->getFieldNames()));
    }

    /// Helper function to add a default emit operator
    /// This is used only when the wrapped operator does not already provide an emit
    /// @note Once we have refactored the memory layout and schema we can get rid of the configured buffer size.
    /// Do not add further parameters here that should be part of the QueryOptimizerConfiguration.
    void addDefaultEmit(const std::shared_ptr<Pipeline>& pipeline, const PhysicalOperatorWrapper& wrappedOp, uint64_t configuredBufferSize)
    {
        PRECONDITION(pipeline->isOperatorPipeline(), "Only add emit physical operator to operator pipelines");
        auto schema = wrappedOp.getOutputSchema();
        INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

        auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>( configuredBufferSize, schema.value());
        const auto memoryProvider = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
        /// Create an operator handler for the emit
        const OperatorHandlerId operatorHandlerIndex = getNextOperatorHandlerId();
        pipeline->getOperatorHandlers().emplace(operatorHandlerIndex, std::make_shared<EmitOperatorHandler>());
        pipeline->appendOperator(EmitPhysicalOperator(operatorHandlerIndex, memoryProvider));
    }

    void handleStartNewPipeline(const Action& action,
                                const std::shared_ptr<PhysicalOperatorWrapper>& op,
                                std::shared_ptr<Pipeline>& currentPipeline,
                                OperatorPipelineMap& pipelineMap)
    {
        if (action.appendEmit)
        {
            addDefaultEmit(currentPipeline, *op, bufferSize);
        }

        /// Create the new pipeline:
        auto newPipe = std::make_shared<Pipeline>(op->getPhysicalOperator());
        pipelineMap.emplace(op->getPhysicalOperator().getId(), newPipe);
        currentPipeline->addSuccessor(newPipe, currentPipeline);

        /// Switch "currentPipeline" in the FSM to point at newPipe:
        currentPipeline = newPipe;

        /// If the action says we need a default scan on entry:
        if (action.prependScan)
        {
            addDefaultScan(currentPipeline, *op, bufferSize);
        }
    }

    // Handler for "just append this operator into the existing pipeline"
    void handleAppendOperator(const Action& action,
                              const std::shared_ptr<PhysicalOperatorWrapper>& op,
                              std::shared_ptr<Pipeline>& currentPipeline,
                              OperatorPipelineMap& pipelineMap)
    {
        currentPipeline->appendOperator(op->getPhysicalOperator());
        if (op->getHandler() && op->getHandlerId()) {
            currentPipeline->getOperatorHandlers()
                           .emplace(op->getHandlerId().value(),
                                    op->getHandler().value());
        }
    }

    void handleCustomEmit(const Action& action,
                          const std::shared_ptr<PhysicalOperatorWrapper>& op,
                          std::shared_ptr<Pipeline>& currentPipeline,
                          OperatorPipelineMap& pipelineMap)
    {
        addDefaultEmit(currentPipeline, *op, bufferSize);
    }

    void handleCustomScan(const Action& action,
                          const std::shared_ptr<PhysicalOperatorWrapper>& op,
                          std::shared_ptr<Pipeline>& currentPipeline,
                          OperatorPipelineMap& pipelineMap)
    {
        /// TODO
    }

    static const std::vector<Transition> kTransitions =
    {
        /// 1
        { State::Idle,  Event::Source, State::InPipeline,
          Action{.prependScan = false, .appendEmit = false, .startNewPipeline = true, .appendOperator = false}, &PipelineBuilderFSM::handleStartNewPipeline },
        /// 2
        { State::InPipeline, Event::CustomScan, State::InPipeline,
          Action{.prependScan = true, .appendEmit = false, .startNewPipeline = true, .appendOperator = false}, &PipelineBuilderFSM::handleCustomScan },
        /// 3
        { State::InPipeline, Event::CustomEmit, State::AfterEmit,
          Action{.prependScan = false, .appendEmit = false, .startNewPipeline = false, .appendOperator = true}, &PipelineBuilderFSM::handleCustomEmit },
        /// 4
        { State::AfterEmit, Event::FusibleOperator, State::AfterEmit,
          Action{.prependScan = true, .appendEmit = false, .startNewPipeline = true, .appendOperator = false}, &PipelineBuilderFSM::handleStartNewPipeline },
        /// 5
        { State::InPipeline,  Event::Sink, State::InPipeline,
          Action{.prependScan = false, .appendEmit = true, .startNewPipeline = true, .appendOperator = false}, &PipelineBuilderFSM::handleStartNewPipeline },
        /// 6
        { State::InPipeline,  Event::FusibleOperator, State::InPipeline,
          Action{.prependScan = false, .appendEmit = false, .startNewPipeline = false, .appendOperator = true}, &PipelineBuilderFSM::handleAppendOperator },
        /// 7
        { State::InPipeline,  Event::FusibleOperator, State::AfterEmit,
          Action{.prependScan = false, .appendEmit = true, .startNewPipeline = true, .appendOperator = false}, &PipelineBuilderFSM::handleAppendOperator }
    };

public:

    PipelineBuilderFSM() : state(State::Idle) {}

    /// process one operator, returns list of new pipelines you need to recurse on
    std::vector<std::shared_ptr<Pipeline>>
    onOperator(const std::shared_ptr<PhysicalOperatorWrapper>& op,
               std::shared_ptr<Pipeline> currentPipeline,
               OperatorPipelineMap& pipelineMap)
    {
        auto [nextState, action] = transition(state, op);
        state = nextState;
        applyAction(action, op, currentPipeline, pipelineMap);
        return getNextPipelinesForRecurse(action, op, currentPipeline, pipelineMap);
    }

private:
    State state;

    std::tuple<State, Action> transition(State state, Operator op)
    {
        auto findTransition = [&](State state, Event event)
        {
            for (auto &t: kTransitions)
            {
                if (t.from == state and t.onEvent == event)
                {
                    return &t;
                }
            }
            /// TODO Error state
        };
    }

    void applyAction(Action action, Operator op, std::shared_ptr<Pipeline> pipeline, OperatorPipelineMap& pipelineMap)
    {

    }

    std::vector<std::shared_ptr<Pipeline>> getNextPipelinesForRecurse(Action action, Operator op, std::shared_ptr<Pipeline> pipeline, OperatorPipelineMap& pipelineMap);

    uint64_t bufferSize = 1000;
};


// void buildPipelineWithFsm(
//   const std::shared_ptr<PhysicalOperatorWrapper>& root,
//   const std::shared_ptr<Pipeline>& rootPipeline,
//   OperatorPipelineMap& pipelineMap,
//   uint64_t configuredBufferSize)
// {
//     PipelineBuilderFsm fsm(configuredBufferSize);
//     pipelineMap.emplace(root->getId(), rootPipeline);
//     for (auto& child : root->getChildren()) {
//         recurse(child, rootPipeline);
//     }
// }
//
// void recurse(
//   const std::shared_ptr<PhysicalOperatorWrapper>& node,
//   std::shared_ptr<Pipeline> currentPipeline)
// {
//     auto newPipelines = fsm.onOperator(node, /*policy=*/, currentPipeline, pipelineMap);
//     for (auto& np : newPipelines) {
//         for (auto& child : node->getChildren()) {
//             recurse(child, np);
//         }
//     }
// }

}
