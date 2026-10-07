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

#include <SemanticFilterPhysicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <fmt/format.h>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <SemanticBackend.hpp>
#include <SemanticFilterCodec.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticOperatorState.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES::detail
{

struct SemanticFilterState final : SemanticOperatorState<SemanticFilterCodec>
{
    using SemanticOperatorState::SemanticOperatorState;

    void process(const WorkerThreadId thread)
    {
        auto& slot = getSlot(thread);
        /// Never fails: an unusable answer drops the record.
        auto result = std::move(codec.parse(roundTrip(slot), slot.rows).front());
        slot.passed = result.passed;
        slot.answers = std::move(result.mapAnswers);
    }
};

}

namespace NES
{

namespace
{

using detail::SemanticFilterState;

void setupSemanticFilter(SemanticFilterState* state, PipelineExecutionContext* pec)
{
    state->setup(pec->getNumberOfWorkerThreads());
}

void terminateSemanticFilter(SemanticFilterState* state)
{
    state->terminate();
}

void setFilterInput(
    SemanticFilterState* state, const WorkerThreadId thread, const uint64_t fieldIndex, const int8_t* content, const uint64_t size)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED bytes are the field's text
    state->getSlot(thread).rows.front().fields[fieldIndex].second.assign(reinterpret_cast<const char*>(content), size);
}

bool filterRecord(SemanticFilterState* state, const WorkerThreadId thread)
{
    state->process(thread);
    return state->getSlot(thread).passed;
}

const int8_t* getFilterAnswer(SemanticFilterState* state, const WorkerThreadId thread, const uint64_t step)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) char* to int8_t* for nautilus::memcpy
    return reinterpret_cast<const int8_t*>(state->getSlot(thread).answers[step].data());
}

uint64_t getFilterAnswerSize(SemanticFilterState* state, const WorkerThreadId thread, const uint64_t step)
{
    return state->getSlot(thread).answers[step].size();
}

}

SemanticFilterPhysicalOperator::SemanticFilterPhysicalOperator(
    SemanticBackendProvider backendProvider,
    SemanticModelConfig config,
    std::vector<QualifiedIdentifier> inputFields,
    std::vector<QualifiedIdentifier> outputFields)
    : inputFields(std::move(inputFields)), outputFields(std::move(outputFields))
{
    const auto mapSteps = static_cast<size_t>(
        std::ranges::count(config.steps, SemanticStep::Kind::MAP, [](const SemanticStep& step) { return step.kind; }));
    PRECONDITION(
        this->outputFields.size() == mapSteps,
        "SemanticFilter writes one output field per map step, but got {} fields for {} map steps",
        this->outputFields.size(),
        mapSteps);

    std::vector<std::string> inputNames;
    inputNames.reserve(this->inputFields.size());
    for (const auto& field : this->inputFields)
    {
        inputNames.push_back(fmt::format("{}", field));
    }
    state = std::make_shared<SemanticFilterState>(std::move(backendProvider), config, std::move(inputNames), mapSteps);
}

void SemanticFilterPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    nautilus::invoke(setupSemanticFilter, nautilus::val<SemanticFilterState*>(state.get()), executionCtx.pipelineContext);
}

void SemanticFilterPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto statePtr = nautilus::val<SemanticFilterState*>(state.get());
    /// static_val unrolls at trace time, so the host-side vector lookups are legal in the traced body.
    for (nautilus::static_val<size_t> i = 0; i < inputFields.size(); ++i)
    {
        const auto value = record.read(inputFields.at(i)).getRawValueAs<VariableSizedData>();
        nautilus::invoke(setFilterInput, statePtr, ctx.workerThreadId, nautilus::val<uint64_t>(i), value.getContent(), value.getSize());
    }
    const nautilus::val<bool> passed = nautilus::invoke(filterRecord, statePtr, ctx.workerThreadId);
    if (passed)
    {
        /// Only a fused operator has MAP steps to write; allocated at the answer's exact size.
        for (nautilus::static_val<size_t> i = 0; i < outputFields.size(); ++i)
        {
            const auto step = nautilus::val<uint64_t>(i);
            const auto answer = nautilus::invoke(getFilterAnswer, statePtr, ctx.workerThreadId, step);
            const auto size = nautilus::invoke(getFilterAnswerSize, statePtr, ctx.workerThreadId, step);
            auto output = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(size);
            nautilus::memcpy(output.getContent(), answer, size);
            record.write(outputFields.at(i), VarVal(output));
        }
        executeChild(ctx, record);
    }
}

void SemanticFilterPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(terminateSemanticFilter, nautilus::val<SemanticFilterState*>(state.get()));
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> SemanticFilterPhysicalOperator::getChild() const
{
    return child;
}

void SemanticFilterPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
