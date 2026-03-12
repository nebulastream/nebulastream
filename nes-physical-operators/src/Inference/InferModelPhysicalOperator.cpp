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

#include <InferModelPhysicalOperator.hpp>

#include <ranges>
#include <Inference/IREEAdapter.hpp>
#include <Inference/IREEInferenceOperatorHandler.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

InferModelPhysicalOperator::InferModelPhysicalOperator(
    OperatorHandlerId handlerId, std::vector<std::string> inputFieldNames, std::vector<std::string> outputFieldNames, bool varsizedInput)
    : handlerId(handlerId)
    , inputFieldNames(std::move(inputFieldNames))
    , outputFieldNames(std::move(outputFieldNames))
    , varsizedInput(varsizedInput)
{
}

namespace
{
void addFloatValueToModel(int index, float value, OperatorHandler* handler, WorkerThreadId thread)
{
    auto* inferHandler = static_cast<Inference::IREEInferenceOperatorHandler*>(handler);
    auto adapter = inferHandler->getIREEAdapter(thread);
    adapter->addModelInput(index, value);
}

/// Copies raw VARSIZED bytes directly into the model input buffer (no decoding).
void addVarsizedInputToModel(int8_t* data, uint64_t size, OperatorHandler* handler, WorkerThreadId thread)
{
    auto* inferHandler = static_cast<Inference::IREEInferenceOperatorHandler*>(handler);
    auto adapter = inferHandler->getIREEAdapter(thread);
    adapter->addModelInput(std::span<std::byte>(reinterpret_cast<std::byte*>(data), size));
}

void applyModel(OperatorHandler* handler, WorkerThreadId thread)
{
    auto* inferHandler = static_cast<Inference::IREEInferenceOperatorHandler*>(handler);
    auto adapter = inferHandler->getIREEAdapter(thread);
    adapter->infer();
}

float getFloatValueFromModel(int index, OperatorHandler* handler, WorkerThreadId thread)
{
    auto* inferHandler = static_cast<Inference::IREEInferenceOperatorHandler*>(handler);
    auto adapter = inferHandler->getIREEAdapter(thread);
    return adapter->getResultAt(index);
}
}

void InferModelPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    nautilus::invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->start(*pec, 0); },
        executionCtx.getGlobalOperatorHandler(handlerId),
        executionCtx.pipelineContext);
}

void InferModelPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto inferModelHandler = ctx.getGlobalOperatorHandler(handlerId);

    if (varsizedInput)
    {
        /// Single VARSIZED input: read raw bytes and feed directly to model
        VarVal value = record.read(inputFieldNames.at(0));
        auto varSized = value.getRawValueAs<VariableSizedData>();
        nautilus::invoke(addVarsizedInputToModel, varSized.getContent(), varSized.getSize(), inferModelHandler, ctx.workerThreadId);
    }
    else
    {
        for (nautilus::static_val<size_t> i = 0; i < inputFieldNames.size(); ++i)
        {
            VarVal value = record.read(inputFieldNames.at(nautilus::static_val<int>(i)));
            nautilus::invoke(
                addFloatValueToModel,
                nautilus::val<int>(i),
                value.getRawValueAs<nautilus::val<float>>(),
                inferModelHandler,
                ctx.workerThreadId);
        }
    }

    nautilus::invoke(applyModel, inferModelHandler, ctx.workerThreadId);

    for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
    {
        VarVal result = VarVal(nautilus::invoke(getFloatValueFromModel, nautilus::val<int>(i), inferModelHandler, ctx.workerThreadId));
        record.write(outputFieldNames.at(i), result);
    }

    executeChild(ctx, record);
}

void InferModelPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->stop(QueryTerminationType::Graceful, *pec); },
        executionCtx.getGlobalOperatorHandler(handlerId),
        executionCtx.pipelineContext);
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> InferModelPhysicalOperator::getChild() const
{
    return child;
}

void InferModelPhysicalOperator::setChild(PhysicalOperator newChild)
{
    this->child = std::move(newChild);
}

}
