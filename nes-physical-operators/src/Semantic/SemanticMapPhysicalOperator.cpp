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

#include <SemanticMapPhysicalOperator.hpp>

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
#include <SemanticMapCodec.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticOperatorState.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES::detail
{

struct SemanticMapState final : SemanticOperatorState<SemanticMapCodec>
{
    using SemanticOperatorState::SemanticOperatorState;

    void process(const WorkerThreadId thread)
    {
        auto& slot = getSlot(thread);
        /// Never fails: an unusable answer writes every step's default value.
        slot.answers = codec.parse(roundTrip(slot), slot.rows).front();
    }
};

}

namespace NES
{

namespace
{

using detail::SemanticMapState;

void setupSemanticMap(SemanticMapState* state, PipelineExecutionContext* pec)
{
    state->setup(pec->getNumberOfWorkerThreads());
}

void terminateSemanticMap(SemanticMapState* state)
{
    state->terminate();
}

void setInput(SemanticMapState* state, const WorkerThreadId thread, const uint64_t fieldIndex, const int8_t* content, const uint64_t size)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED bytes are the field's text
    state->getSlot(thread).rows.front().fields[fieldIndex].second.assign(reinterpret_cast<const char*>(content), size);
}

void processRecord(SemanticMapState* state, const WorkerThreadId thread)
{
    state->process(thread);
}

const int8_t* getAnswer(SemanticMapState* state, const WorkerThreadId thread, const uint64_t step)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) char* to int8_t* for nautilus::memcpy
    return reinterpret_cast<const int8_t*>(state->getSlot(thread).answers[step].data());
}

uint64_t getAnswerSize(SemanticMapState* state, const WorkerThreadId thread, const uint64_t step)
{
    return state->getSlot(thread).answers[step].size();
}

}

SemanticMapPhysicalOperator::SemanticMapPhysicalOperator(
    SemanticBackendProvider backendProvider,
    SemanticModelConfig config,
    std::vector<QualifiedIdentifier> inputFields,
    std::vector<QualifiedIdentifier> outputFields)
    : inputFields(std::move(inputFields)), outputFields(std::move(outputFields))
{
    PRECONDITION(
        this->outputFields.size() == config.steps.size(),
        "SemanticMap writes one output field per step, but got {} fields for {} steps",
        this->outputFields.size(),
        config.steps.size());
    std::vector<std::string> inputNames;
    inputNames.reserve(this->inputFields.size());
    for (const auto& field : this->inputFields)
    {
        inputNames.push_back(fmt::format("{}", field));
    }
    const auto numberOfSteps = config.steps.size();
    state = std::make_shared<SemanticMapState>(std::move(backendProvider), config, std::move(inputNames), numberOfSteps);
}

void SemanticMapPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    nautilus::invoke(setupSemanticMap, nautilus::val<SemanticMapState*>(state.get()), executionCtx.pipelineContext);
}

void SemanticMapPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto statePtr = nautilus::val<SemanticMapState*>(state.get());

    /// static_val unrolls at trace time, so the host-side vector lookups are legal in the traced body.
    for (nautilus::static_val<size_t> i = 0; i < inputFields.size(); ++i)
    {
        const auto value = record.read(inputFields.at(i)).getRawValueAs<VariableSizedData>();
        nautilus::invoke(setInput, statePtr, ctx.workerThreadId, nautilus::val<uint64_t>(i), value.getContent(), value.getSize());
    }

    nautilus::invoke(processRecord, statePtr, ctx.workerThreadId);

    /// The answer's length is known once the round trip is done, so each field is allocated at its
    /// exact size rather than at an estimated upper bound.
    for (nautilus::static_val<size_t> i = 0; i < outputFields.size(); ++i)
    {
        const auto step = nautilus::val<uint64_t>(i);
        const auto answer = nautilus::invoke(getAnswer, statePtr, ctx.workerThreadId, step);
        const auto size = nautilus::invoke(getAnswerSize, statePtr, ctx.workerThreadId, step);
        auto output = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(size);
        nautilus::memcpy(output.getContent(), answer, size);
        record.write(outputFields.at(i), VarVal(output));
    }

    executeChild(ctx, record);
}

void SemanticMapPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(terminateSemanticMap, nautilus::val<SemanticMapState*>(state.get()));
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> SemanticMapPhysicalOperator::getChild() const
{
    return child;
}

void SemanticMapPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
